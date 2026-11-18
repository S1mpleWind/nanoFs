// spec · A single Raft node (Raft paper Fig. 2). Owns consensus state guarded
// by one mutex and one background thread that fires election timeouts (as
// follower/candidate) and heartbeats (as leader). RPCs go out through the
// injected RpcNetwork; incoming RPCs land on handle_* (called by peers).
#pragma once
#include <atomic>
#include <condition_variable>
#include <mutex>
#include <optional>
#include <random>
#include <thread>
#include <unordered_map>
#include <vector>
#include "nanoraft/raft_protocol.h"
#include "nanoraft/rpc_network.h"
#include "nanoraft/raft_state_machine.h"
#include "nanoraft/raft_storage.h"

namespace nanoraft {

enum class Role { Follower, Candidate, Leader };

class RaftNode {
 public:
  // id: this node's own id, in [0, n). n: the cluster size — the peer set is the
  // fixed id range 0..n-1 (this node included), so a majority is `count * 2 > n`.
  // storage (optional): if non-null, the durable state (term/vote/log + snapshot) is loaded
  // from it at construction and re-saved on every change. nullptr = in-memory only.
  // compact_threshold: compact once (last_applied_ - snapshot_index_) exceeds it.
  RaftNode(int id, int n, RpcNetwork* net, RaftStateMachine* state_machine,
           RaftStorage* storage = nullptr, int compact_threshold = 64);
  ~RaftNode();

  void start();   // launch the background timer thread
  void stop();    // signal + join the background thread (idempotent)

  bool is_leader() const;
  int  current_term() const;

  // Incoming RPC handlers (invoked by peers via RpcNetwork). Thread-safe.
  RequestVoteReply     handle_request_vote(const RequestVoteArgs& args);
  AppendEntriesReply   handle_append_entries(const AppendEntriesArgs& args);
  InstallSnapshotReply handle_install_snapshot(const InstallSnapshotArgs& args);

  // Result of proposing a command to a node. `is_leader` is false (and index/term
  // are advisory) if this node was not the leader; the client should redirect.
  struct ProposeResult { int index; int term; bool is_leader; };

  // Append `command` to the leader's log for replication. Returns the 1-based log
  // index it was placed at, the current term, and is_leader = true. On a non-leader
  // returns {0, current_term_, false} and appends nothing. Thread-safe.
  ProposeResult propose(const std::string& command);

  int commit_index() const;   // highest log index known committed (0 = none)
  int snapshot_index() const; // last Raft index folded into the snapshot (0 = none)

  // The log still in memory (everything past snapshot_index_), oldest first, one
  // "<term>:<command>" string per entry — e.g. {"1:a", "2:c"} for a log holding
  // "a" from term 1 then "c" from term 2. Read-only; useful in tests and when
  // debugging your own node. Thread-safe (unlike the private log helpers, which
  // require mu_ held).
  std::vector<std::string> log_contents() const;

  // Block until entry `index` is applied, then return its apply() result. `term`
  // is the term propose() handed back: if the entry applied at `index` carries a
  // different term, the proposal was overwritten by another leader and never
  // committed, so this returns nullopt — Raft §8's check before responding to a
  // client. Also nullopt on timeout or if the node stopped. Thread-safe. Each
  // result is consumed once (the caller that proposed the entry collects it).
  std::optional<std::string> await_applied(int index, int term,
                                           std::chrono::milliseconds timeout);
  int leader_hint() const;    // last known leader id (-1 if unknown)

 private:
  void recover_from_storage();   // PROVIDED: load the durable state (term/vote/log + snapshot) from storage_ and restore state_machine_. Called once by the constructor; no-op if storage_ == nullptr.
  void run_timer();                       // background loop
  void start_election();                  // becomes candidate, gathers votes, takes leadership on a majority
  void send_heartbeats();                 // PROVIDED: one replication round — per peer pick one RPC (build_* below), send it on its own thread, hand the reply to the matching on_*
  AppendEntriesArgs   build_append_entries(int peer) const;    // PROVIDED: the AppendEntries for a peer whose next_index_ is still in the log — everything from next_index_[peer] on, empty (a heartbeat) if that peer is caught up. Precondition: mu_ held, role_ == Leader, next_index_[peer] > snapshot_index_.
  InstallSnapshotArgs build_install_snapshot() const;          // PROVIDED: the InstallSnapshot for a peer whose next_index_ fell at/below snapshot_index_ (we compacted the entries it needs); the same blob for every peer. Precondition: mu_ held, role_ == Leader.
  void on_append_entries_reply(int peer, const AppendEntriesArgs& args, const AppendEntriesReply& reply);   // leader side: a peer answered an AppendEntries we sent — fold the reply into match_/next_/commit. Takes mu_ itself. (handle_* = an RPC a peer sent us; on_* = a reply to an RPC we sent.)
  void on_install_snapshot_reply(int peer, const InstallSnapshotArgs& args, const InstallSnapshotReply& reply);   // leader side: a peer answered an InstallSnapshot we sent — the snapshot's last_included_index is now replicated. Takes mu_ itself.
  void apply_committed();        // apply (last_applied_, commit_index_] to state_machine_ in order. Precondition: mu_ held.
  void persist();                // save {current_term_, voted_for_, log_, snapshot_index_/term_/bytes} to storage_ in one atomic write. Precondition: mu_ held. No-op if storage_ == nullptr.
  void advance_commit_index();   // leader: raise commit_index_ to the highest majority-replicated current-term index. Precondition: mu_ held.
  void maybe_compact();          // PROVIDED: compact the applied prefix past the threshold into a snapshot. Called by apply_committed. Precondition: mu_ held.
  std::chrono::milliseconds random_timeout();
  int first_index() const;                // snapshot_index_ + 1 (first Raft index still in log_)
  int last_log_index() const;             // snapshot_index_ + log_.size() (0 if no snapshot and empty)
  int last_log_term() const;              // term of the last entry, or snapshot_term_ if log_ empty
  int term_at(int index) const;           // term of Raft `index`; snapshot_term_ at snapshot_index_. Precondition: has_entry(index) || index == snapshot_index_.
  bool has_entry(int index) const;        // index is in (snapshot_index_, last_log_index()]

  const int id_;   // this node's id, in [0, n_)
  const int n_;    // cluster size; peers are ids 0..n_-1 (self included), majority = count*2 > n_
  RpcNetwork* net_;        // non-owning; node<->node transport (send_* to a peer id)
  RaftStateMachine* state_machine_;   // non-owning; the replicated app committed entries are applied to
  RaftStorage* storage_;   // non-owning; may be null (no persistence)

  mutable std::mutex mu_;         // guards every member below; never hold it across a net_->send_*
  std::condition_variable cv_;    // wakes the timer thread + await_applied waiters
  Role role_ = Role::Follower;    // Fig. 2 `state`
  int current_term_ = 0;          // latest term seen (durable)
  int voted_for_ = -1;                    // -1 = none this term
  std::vector<LogEntry> log_;             // offset by snapshot_index_: log_[0] is Raft index snapshot_index_+1
  int snapshot_index_ = 0;   // last Raft index folded into the snapshot (0 = none)
  int snapshot_term_ = 0;    // term of the entry at snapshot_index_ (0 = none)
  std::string snapshot_;     // latest snapshot bytes (mirrors storage_)
  int compact_threshold_ = 64;   // compact once (last_applied_ - snapshot_index_) exceeds this
  int commit_index_ = 0;   // highest log index known committed
  int last_applied_ = 0;   // highest log index applied to state_machine_
  int leader_id_ = -1;     // last known leader id (-1 = unknown), for client redirect
  std::unordered_map<int, std::pair<int, std::string>> apply_results_;  // index -> {entry term, apply() result}, awaited then consumed
  // Both sized n_ and indexed by node id (the self slot included: match_index_[id_]
  // is the leader's own last log index, so it counts toward its own commit majority).
  std::vector<int> next_index_;    // leader only: per-peer index of the next entry to send
  std::vector<int> match_index_;   // leader only: per-peer highest index known replicated

  std::chrono::steady_clock::time_point last_heard_;  // last valid leader/candidate contact
  std::atomic<bool> running_{false};   // false before start() / after stop(): the timer loop exits
  std::thread timer_;                  // the one background thread (run_timer)
  std::mt19937 rng_;                   // seeded per node id — random_timeout() only
};

}  // namespace nanoraft
