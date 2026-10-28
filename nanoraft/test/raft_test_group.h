// test fixture · spins up N RaftNodes wired to one MockNetwork, with helpers
// for the election/replication invariants. In-memory only (no disk in P1a).
#pragma once
#include <chrono>
#include <memory>
#include <thread>
#include <vector>
#include "mock_network.h"
#include "nanoraft/raft.h"

namespace nanoraft {

class RaftGroup {
 public:
  explicit RaftGroup(int n, int compact_threshold = 64)
      : n_(n), compact_threshold_(compact_threshold) {
    net_ = std::make_unique<MockNetwork>();
    for (int i = 0; i < n_; ++i) {
      sms_.push_back(std::make_unique<ListStateMachine>());
      stores_.push_back(std::make_unique<MemoryStorage>());
      nodes_.push_back(std::make_unique<RaftNode>(i, n_, net_.get(), sms_[i].get(),
                                                  stores_[i].get(), compact_threshold_));
      net_->register_node(i, nodes_[i].get());
    }
  }
  ~RaftGroup() { stop_all(); }

  void start_all() { for (auto& nd : nodes_) nd->start(); }
  void stop_all()  { for (auto& nd : nodes_) if (nd) nd->stop(); }

  void disconnect(int i) { net_->disconnect(i); }
  void reconnect(int i)  { net_->reconnect(i); }

  // Ids of currently-CONNECTED nodes that believe they are leader. A
  // disconnected old leader still thinks it is leader (correct Raft) but is
  // unreachable, so tests must ignore it — mirrors the standard Raft test
  // harness (count the leader only among reachable nodes).
  std::vector<int> leaders() {
    std::vector<int> ls;
    for (int i = 0; i < n_; ++i)
      if (net_->is_connected(i) && nodes_[i]->is_leader()) ls.push_back(i);
    return ls;
  }

  // Poll until exactly one connected node is leader, or the timeout elapses.
  // Returns that leader's id, or -1 on timeout.
  int wait_for_one_leader(std::chrono::milliseconds timeout) {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
      auto ls = leaders();
      if (ls.size() == 1) return ls[0];
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return -1;
  }

  // Poll until every CONNECTED node reports the same term, and return it; -1 on
  // timeout (the standard Raft harness's checkTerms). Disconnected nodes are
  // ignored: an isolated node keeps timing out and bumping its term, and would
  // never match the majority. Polls rather than sampling once because the
  // cluster converges on the new term one vote at a time, not atomically at the
  // moment a leader appears.
  int wait_for_agreed_term(std::chrono::milliseconds timeout) {
    auto agreed_now = [&] {
      int term = -1;
      for (int i = 0; i < n_; ++i) {
        if (!net_->is_connected(i)) continue;
        int node_term = nodes_[i]->current_term();
        if (term != -1 && node_term != term) return -1;
        term = node_term;
      }
      return term;
    };
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
      int term = agreed_now();
      if (term > 0) return term;
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return -1;
  }

  RaftNode* node(int i) { return nodes_[i].get(); }
  int size() const { return n_; }

  // Id of one node that is not `leader`. Tests that just need "a follower" say
  // so instead of open-coding (leader + 1) % n.
  int some_follower(int leader) const { return (leader + 1) % n_; }

  // The other n-1 ids, ordered by distance from `node`: other_nodes(l)[0] is
  // some_follower(l). Use it to isolate a node or to walk its peers.
  std::vector<int> other_nodes(int node) const {
    std::vector<int> others;
    for (int step = 1; step < n_; ++step) others.push_back((node + step) % n_);
    return others;
  }

  MockNetwork* net() { return net_.get(); }   // for loss / reachability control

  // Keep proposing `cmd` until it is applied in agreement on >= n nodes —
  // retries across leader changes and entries lost to leader turnover.
  // Returns the applied 1-based index, or -1 on timeout. A retried command
  // may commit twice at different indexes; callers assert agreement, not
  // exact counts.
  int propose_until_applied(const std::string& cmd, int n,
                           std::chrono::milliseconds timeout) {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
      int idx = propose_to_leader(cmd);
      if (idx > 0 && wait_applied(idx, n, std::chrono::milliseconds(2000)) == cmd)
        return idx;
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    return -1;
  }

  // Simulate a fail-stop crash: remove the node from the network (so no peer can
  // reach a dangling pointer), stop its threads, and destroy it. Its MemoryStorage
  // and the group's other state survive.
  void crash(int i) {
    net_->set_node(i, nullptr);
    net_->disconnect(i);
    nodes_[i]->stop();
    nodes_[i].reset();
  }

  // Restart a crashed node from its persisted storage with a FRESH state machine
  // (the committed log re-applies as commits flow again — Raft rebuilds volatile
  // state, incl. the applied SM, after restart).
  void restart(int i) {
    sms_[i] = std::make_unique<ListStateMachine>();
    nodes_[i] = std::make_unique<RaftNode>(i, n_, net_.get(), sms_[i].get(),
                                           stores_[i].get(), compact_threshold_);
    net_->set_node(i, nodes_[i].get());
    net_->reconnect(i);
    nodes_[i]->start();
  }

  int snapshot_index(int i) { return nodes_[i]->snapshot_index(); }

  // Submit to the single current leader; returns the 1-based index, or -1 if there
  // is not exactly one (connected) leader right now.
  int propose_to_leader(const std::string& cmd) {
    auto ls = leaders();
    if (ls.size() != 1) return -1;
    auto r = nodes_[ls[0]]->propose(cmd);
    return r.is_leader ? r.index : -1;
  }

  // How many nodes have applied a command at 1-based `index`, and the agreed command.
  // Returns {count, command}; {-1, ""} if appliers disagree at that index.
  std::pair<int, std::string> n_applied(int index) {
    std::string cmd;
    int count = 0;
    for (int i = 0; i < n_; ++i) {
      auto ap = sms_[i]->applied();
      if (static_cast<int>(ap.size()) >= index) {
        const std::string& c = ap[index - 1];
        if (count > 0 && c != cmd) return {-1, ""};
        cmd = c;
        ++count;
      }
    }
    return {count, cmd};
  }

  // Poll until at least `n` nodes have applied `index` in agreement; return the
  // agreed command, or "" on timeout / disagreement.
  std::string wait_applied(int index, int n, std::chrono::milliseconds timeout) {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
      auto [count, cmd] = n_applied(index);
      if (count >= n) return cmd;
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return "";
  }

 private:
  int n_;
  int compact_threshold_;
  std::unique_ptr<MockNetwork> net_;
  std::vector<std::unique_ptr<ListStateMachine>> sms_;
  std::vector<std::unique_ptr<MemoryStorage>> stores_;
  std::vector<std::unique_ptr<RaftNode>> nodes_;
};

}  // namespace nanoraft
