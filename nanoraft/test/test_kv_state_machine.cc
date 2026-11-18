#include "mock_network.h"
#include "nanoraft/kv_state_machine.h"
#include <gtest/gtest.h>
#include <chrono>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <vector>

using namespace nanoraft;
using namespace std::chrono_literals;

// §3 has two layers of tests in this file:
//   1. KvStateMachine.* checks the deterministic state machine in isolation.
//   2. RaftKv.* checks the same commands through a three-node Raft cluster.

TEST(KvStateMachine, PutThenGet) {
  KvStateMachine kv;
  kv.apply(encode({KvOp::Put, "a", "1", "c1", 1}));
  EXPECT_EQ(kv.apply(encode({KvOp::Get, "a", "", "c1", 2})), "1");
}

TEST(KvStateMachine, DuplicateReqSeqNotReapplied) {
  KvStateMachine kv;
  kv.apply(encode({KvOp::Put, "a", "1", "c1", 5}));
  kv.apply(encode({KvOp::Put, "a", "2", "c1", 5}));   // same seq -> ignored
  EXPECT_EQ(kv.apply(encode({KvOp::Get, "a", "", "c1", 6})), "1");
}

TEST(KvStateMachine, ScanPrefixSorted) {
  KvStateMachine kv;
  kv.apply(encode({KvOp::Put, "s/2", "b", "c1", 1}));
  kv.apply(encode({KvOp::Put, "s/1", "a", "c1", 2}));
  kv.apply(encode({KvOp::Put, "t/1", "z", "c1", 3}));   // outside prefix
  auto rows = decode_scan(kv.apply(encode({KvOp::Scan, "s/", "", "c1", 4})));
  ASSERT_EQ(rows.size(), 2u);
  EXPECT_EQ(rows[0].first, "s/1");   // sorted by key
  EXPECT_EQ(rows[1].second, "b");
}

TEST(KvStateMachine, SnapshotRestoreRoundTrips) {
  KvStateMachine a;
  a.apply(encode({KvOp::Put, "k", "v", "c1", 1}));
  KvStateMachine b;
  b.restore(a.snapshot());
  EXPECT_EQ(b.apply(encode({KvOp::Get, "k", "", "c1", 2})), "v");
  // dedup table also survives: a replayed seq is a no-op after restore
  b.apply(encode({KvOp::Put, "k", "w", "c1", 1}));
  EXPECT_EQ(b.apply(encode({KvOp::Get, "k", "", "c1", 3})), "v");
}

// A minimal in-memory cluster of RaftNodes over KvStateMachine instances
// (RaftGroup is hard-wired to ListStateMachine, so the KV app gets its own
// tiny fixture). It exercises propose -> commit -> apply -> result without
// sockets.
class KvCluster {
 public:
  explicit KvCluster(int size) : size_(size) {
    for (int node = 0; node < size_; ++node) {
      state_machines_.push_back(std::make_unique<KvStateMachine>());
      nodes_.push_back(std::make_unique<RaftNode>(
          node, size_, &network_, state_machines_[node].get()));
      network_.register_node(node, nodes_[node].get());
    }
  }

  ~KvCluster() {
    for (auto& node : nodes_) node->stop();
  }

  void start() {
    for (auto& node : nodes_) node->start();
  }

  RaftNode* node(int i) { return nodes_[i].get(); }

  int wait_for_leader(std::chrono::milliseconds timeout) {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
      for (int node = 0; node < size_; ++node) {
        if (nodes_[node]->is_leader()) return node;
      }
      std::this_thread::sleep_for(20ms);
    }
    return -1;
  }

  // Run one command through the current leader, retrying across leader changes.
  std::optional<std::string> apply(
      const KvCommand& command, std::chrono::milliseconds timeout) {
    std::string encoded_command = encode(command);
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
      for (auto& node : nodes_) {
        auto proposal = node->propose(encoded_command);
        if (!proposal.is_leader) continue;
        // A propose landing on a stale leader never commits, so give await
        // only the remaining budget — a full-budget wait would burn the whole
        // deadline on one node and make the cross-leader retry loop dead code.
        auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
            deadline - std::chrono::steady_clock::now());
        if (remaining <= 0ms) return std::nullopt;
        if (auto result =
                node->await_applied(proposal.index, proposal.term, remaining))
          return result;
      }
      std::this_thread::sleep_for(20ms);
    }
    return std::nullopt;
  }

 private:
  int size_;
  MockNetwork network_;
  std::vector<std::unique_ptr<KvStateMachine>> state_machines_;
  std::vector<std::unique_ptr<RaftNode>> nodes_;
};

// Concurrent writes from distinct clients all commit; later linearizable reads
// observe every committed value.
TEST(RaftKv, ConcurrentPutsCommitAndAgree) {
  KvCluster cluster(3);
  cluster.start();
  ASSERT_GE(cluster.wait_for_leader(2s), 0);

  std::vector<std::thread> clients;
  for (int client = 0; client < 4; ++client) {
    clients.emplace_back([&, client] {
      std::string suffix = std::to_string(client);
      cluster.apply(
          KvCommand{KvOp::Put, "key-" + suffix, "value-" + suffix,
                    "client-" + suffix, 1},
          3s);
    });
  }
  for (auto& client : clients) client.join();

  for (int client = 0; client < 4; ++client) {
    std::string suffix = std::to_string(client);
    auto value = cluster.apply(
        KvCommand{KvOp::Get, "key-" + suffix, "", "", 0}, 3s);
    ASSERT_TRUE(value.has_value());
    EXPECT_EQ(*value, "value-" + suffix);
  }
}

// Replaying the same (client_id, req_seq) is exactly once across the replicated
// state machine: the second value cannot overwrite the first.
TEST(RaftKv, RetransmittedRequestAppliesOnce) {
  KvCluster cluster(3);
  cluster.start();
  ASSERT_GE(cluster.wait_for_leader(2s), 0);

  ASSERT_TRUE(cluster
                  .apply(KvCommand{KvOp::Put, "a", "1", "client", 7}, 3s)
                  .has_value());
  ASSERT_TRUE(cluster
                  .apply(KvCommand{KvOp::Put, "a", "2", "client", 7}, 3s)
                  .has_value());

  auto value =
      cluster.apply(KvCommand{KvOp::Get, "a", "", "", 0}, 3s);
  ASSERT_TRUE(value.has_value());
  EXPECT_EQ(*value, "1");

  // await_applied's other arm — the contract §4's kv_* plane relies on:
  // waiting on an index that never commits times out instead of blocking.
  EXPECT_FALSE(cluster.node(0)->await_applied(9999, 1, 300ms).has_value());
}
