// Replaces the real RPC network in tests: an in-process Raft network with
// per-node reachability control.
// A message is delivered iff BOTH endpoints are currently connected, so
// `disconnect(i)` unplugs i from EVERY peer — two disconnected nodes cannot
// reach each other either. Delivery is a synchronous call into the target
// node's RPC handler; the SENDER is expected to call from a short-lived thread
// so a slow/dropped peer never wedges its election loop. Disconnect gates
// delivery only — the target's timer threads keep running, so a disconnected
// node goes on bumping its term (an unplugged cable, not a freeze).
#pragma once
#include <mutex>
#include <random>
#include <unordered_map>
#include <vector>
#include "nanoraft/rpc_network.h"
#include "nanoraft/raft.h"
#include "nanoraft/raft_storage.h"

namespace nanoraft {

class MockNetwork : public RpcNetwork {
 public:
  void register_node(int id, RaftNode* node) {
    std::lock_guard<std::mutex> g(mu_);
    nodes_[id] = node;
    connected_[id] = true;
  }
  void disconnect(int id) { std::lock_guard<std::mutex> g(mu_); connected_[id] = false; }
  void reconnect(int id)  { std::lock_guard<std::mutex> g(mu_); connected_[id] = true; }

  // Random message loss: each RPC leg (request, then reply) is independently
  // dropped with probability p. Deterministic under `seed`. 0 = reliable.
  void set_loss(double p, unsigned seed = 1) {
    std::lock_guard<std::mutex> g(mu_);
    loss_ = p;
    rng_.seed(seed);
  }

  bool is_connected(int id) {
    std::lock_guard<std::mutex> g(mu_);
    auto it = connected_.find(id);
    return it != connected_.end() && it->second;
  }

  void set_node(int id, RaftNode* node) {   // nullptr = crashed/absent
    std::lock_guard<std::mutex> g(mu_);
    nodes_[id] = node;
  }

  std::optional<RequestVoteReply>
  send_request_vote(int from, int to, const RequestVoteArgs& args) override {
    std::lock_guard<std::mutex> g(mu_);
    if (!connected_[from] || !connected_[to]) return std::nullopt;
    auto it = nodes_.find(to);
    if (it == nodes_.end() || it->second == nullptr) return std::nullopt;
    if (drop_roll()) return std::nullopt;             // request lost in flight
    auto reply = it->second->handle_request_vote(args);
    if (drop_roll()) return std::nullopt;             // reply lost in flight
    return reply;
  }

  std::optional<AppendEntriesReply>
  send_append_entries(int from, int to, const AppendEntriesArgs& args) override {
    std::lock_guard<std::mutex> g(mu_);
    if (!connected_[from] || !connected_[to]) return std::nullopt;
    auto it = nodes_.find(to);
    if (it == nodes_.end() || it->second == nullptr) return std::nullopt;
    if (drop_roll()) return std::nullopt;             // request lost in flight
    auto reply = it->second->handle_append_entries(args);
    if (drop_roll()) return std::nullopt;             // reply lost in flight
    return reply;
  }

  std::optional<InstallSnapshotReply>
  send_install_snapshot(int from, int to, const InstallSnapshotArgs& args) override {
    std::lock_guard<std::mutex> g(mu_);
    if (!connected_[from] || !connected_[to]) return std::nullopt;
    auto it = nodes_.find(to);
    if (it == nodes_.end() || it->second == nullptr) return std::nullopt;
    if (drop_roll()) return std::nullopt;             // request lost in flight
    auto reply = it->second->handle_install_snapshot(args);
    if (drop_roll()) return std::nullopt;             // reply lost in flight
    return reply;
  }

 private:
  // Precondition: mu_ held.
  bool drop_roll() {
    if (loss_ <= 0.0) return false;
    return std::uniform_real_distribution<double>(0.0, 1.0)(rng_) < loss_;
  }
  double loss_ = 0.0;
  std::mt19937 rng_{1};

  std::mutex mu_;
  std::unordered_map<int, RaftNode*> nodes_;
  std::unordered_map<int, bool> connected_;
};

// Replaces the real replicated app (the KV store) in election/replication
// tests: apply() records commands in order so a test can assert what each
// node committed.
class ListStateMachine : public RaftStateMachine {
 public:
  std::string apply(const std::string& command) override {
    std::lock_guard<std::mutex> g(mu_);
    applied_.push_back(command);
    return "";   // no per-command result
  }
  std::string snapshot() const override {
    std::lock_guard<std::mutex> g(mu_);
    std::string s;
    for (auto& c : applied_) { s += c; s.push_back('\n'); }
    return s;
  }
  void restore(const std::string& snap) override {
    std::lock_guard<std::mutex> g(mu_);
    applied_.clear();
    size_t start = 0;
    while (start < snap.size()) {
      size_t nl = snap.find('\n', start);
      if (nl == std::string::npos) break;
      applied_.push_back(snap.substr(start, nl - start));
      start = nl + 1;
    }
  }
  std::vector<std::string> applied() const {
    std::lock_guard<std::mutex> g(mu_); return applied_;
  }
 private:
  mutable std::mutex mu_;
  std::vector<std::string> applied_;
};

// Replaces FileStorage in tests: an in-memory RaftStorage. Owned by the test /
// RaftGroup (not the node), so it survives a RaftNode's destruction and a
// restart can reload it.
class MemoryStorage : public RaftStorage {
 public:
  void save(const PersistentState& s) override {
    std::lock_guard<std::mutex> g(mu_);
    state_ = s;
    ++saves_;
  }
  PersistentState load() override {
    std::lock_guard<std::mutex> g(mu_);
    return state_;
  }
  int saves() const { std::lock_guard<std::mutex> g(mu_); return saves_; }
 private:
  mutable std::mutex mu_;
  PersistentState state_;
  int saves_ = 0;
};

}  // namespace nanoraft
