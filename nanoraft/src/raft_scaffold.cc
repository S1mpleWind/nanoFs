// PROVIDED — do not edit, do not submit. The scaffolding a Raft node needs but
// that teaches nothing: the timer thread, the randomized timeout, the 1-based
// log-offset helpers, apply_committed / maybe_compact / persist /
// recover_from_storage / await_applied, the two request builders
// (build_append_entries / build_install_snapshot), the accessors, and
// send_heartbeats — the leader's replication round, whose every decision is
// delegated to a function in src/raft.cc.
// Your side of RaftNode — election, replication, persistence call sites,
// snapshots — lives in src/raft.cc (see docs/lab3/lab3-tasks.md). Both files define members
// of the same class; nothing here needs to change for yours to work.
#include "nanoraft/raft.h"

namespace nanoraft {

using namespace std::chrono;

RaftNode::RaftNode(int id, int n, RpcNetwork* net, RaftStateMachine* state_machine,
                   RaftStorage* storage, int compact_threshold)
    : id_(id), n_(n), net_(net), state_machine_(state_machine), storage_(storage),
      compact_threshold_(compact_threshold),
      rng_(std::random_device{}() ^ id) {
  recover_from_storage();
}

// Crash recovery, the mirror of persist(): load() the durable state back into
// current_term_ / voted_for_ / log_, so a restarted node keeps the identity and
// log it had before. When the saved snapshot is non-empty, restore() the state
// machine from it and set last_applied_ AND commit_index_ to snapshot_index_ —
// that prefix already sits in the state machine, so leaving last_applied_ at 0
// would re-apply it on top of itself. No-op without storage.
void RaftNode::recover_from_storage() {
  if (!storage_) return;
  PersistentState s = storage_->load();
  current_term_ = s.current_term;
  voted_for_ = s.voted_for;
  log_ = s.log;
  snapshot_index_ = s.snapshot_index;
  snapshot_term_ = s.snapshot_term;
  snapshot_ = s.snapshot;
  if (!snapshot_.empty()) {
    state_machine_->restore(snapshot_);
    last_applied_ = snapshot_index_;
    commit_index_ = snapshot_index_;
  }
}

RaftNode::~RaftNode() { stop(); }

void RaftNode::start() {
  running_ = true;
  { std::lock_guard<std::mutex> g(mu_); last_heard_ = steady_clock::now(); }
  timer_ = std::thread([this] { run_timer(); });
}

void RaftNode::stop() {
  bool was = running_.exchange(false);
  if (!was) return;
  cv_.notify_all();
  if (timer_.joinable()) timer_.join();
}

bool RaftNode::is_leader() const {
  std::lock_guard<std::mutex> g(mu_); return role_ == Role::Leader;
}
int RaftNode::current_term() const {
  std::lock_guard<std::mutex> g(mu_); return current_term_;
}

int RaftNode::first_index() const { return snapshot_index_ + 1; }
int RaftNode::last_log_index() const { return snapshot_index_ + static_cast<int>(log_.size()); }
int RaftNode::last_log_term() const { return log_.empty() ? snapshot_term_ : log_.back().term; }
// Term of Raft `index`. Precondition: index == snapshot_index_ or has_entry(index).
int RaftNode::term_at(int index) const {
  if (index == snapshot_index_) return snapshot_term_;
  return log_[index - snapshot_index_ - 1].term;
}
bool RaftNode::has_entry(int index) const {
  return index > snapshot_index_ && index <= last_log_index();
}

milliseconds RaftNode::random_timeout() {
  std::uniform_int_distribution<int> d(300, 600);
  return milliseconds(d(rng_));
}

void RaftNode::run_timer() {
  std::unique_lock<std::mutex> lk(mu_);
  auto timeout = random_timeout();
  while (running_) {
    if (role_ == Role::Leader) {
      lk.unlock();
      send_heartbeats();
      std::this_thread::sleep_for(milliseconds(100));  // heartbeat interval
      lk.lock();
      continue;
    }
    // Follower/Candidate: wait for a timeout window; if no contact, start election.
    cv_.wait_for(lk, timeout);
    if (!running_) break;
    if (role_ == Role::Leader) continue;
    auto since = duration_cast<milliseconds>(steady_clock::now() - last_heard_);
    if (since >= timeout) {
      lk.unlock();
      start_election();
      lk.lock();
      timeout = random_timeout();
    }
  }
}

// Leader: the AppendEntries for a `peer`, carrying everything from
// next_index_[peer] onwards. prev_log_index/term describe the entry it attaches
// after. The precondition puts prev at snapshot_index_ or later, which is exactly
// term_at's domain — so the three cases (a live entry, the snapshot boundary, and
// an empty log with no snapshot, where 0/0 means "attach at the very beginning")
// all come out of one call. A peer that is already caught up yields an empty
// slice — that empty request is the heartbeat.
// Precondition: mu_ held, role_ == Leader, next_index_[peer] > snapshot_index_.
AppendEntriesArgs RaftNode::build_append_entries(int peer) const {
  int ni = next_index_[peer];
  int prev = ni - 1;                                  // 1-based; 0 = before the first entry
  return AppendEntriesArgs{
      .term = current_term_, .leader_id = id_,
      .prev_log_index = prev, .prev_log_term = term_at(prev),
      .entries = std::vector<LogEntry>(log_.begin() + (ni - first_index()), log_.end()),
      .leader_commit = commit_index_};
}

// Leader: the InstallSnapshot for a peer that has fallen behind our compacted
// prefix (Fig. 13) — our whole snapshot, described by the index/term of its last
// entry. The same for every peer (one blob per RPC, no chunking), so it
// takes no peer argument. Precondition: mu_ held, role_ == Leader.
InstallSnapshotArgs RaftNode::build_install_snapshot() const {
  return InstallSnapshotArgs{
      .term = current_term_, .leader_id = id_,
      .last_included_index = snapshot_index_, .last_included_term = snapshot_term_,
      .data = snapshot_};
}

// Leader: one replication round; run_timer sleeps 100ms after it returns.
// Per peer pick exactly one RPC — InstallSnapshot if we compacted away the entry
// it still needs, AppendEntries otherwise — build it under mu_, then send it on
// its own thread. Everything with a decision in it lives in the two on_* reply
// handlers this calls from src/raft.cc; the rest — including
// the two builders above, which only slice the log or copy the snapshot fields —
// is plumbing.
void RaftNode::send_heartbeats() {
  std::vector<std::thread> workers;
  {
    std::lock_guard<std::mutex> g(mu_);
    if (role_ != Role::Leader) return;
    for (int peer = 0; peer < n_; ++peer) {
      if (peer == id_) continue;
      if (next_index_[peer] <= snapshot_index_) {
        InstallSnapshotArgs args = build_install_snapshot();
        workers.emplace_back([this, peer, args] {
          auto reply = net_->send_install_snapshot(id_, peer, args);
          if (reply) on_install_snapshot_reply(peer, args, *reply);
        });
      } else {
        AppendEntriesArgs args = build_append_entries(peer);
        workers.emplace_back([this, peer, args] {
          auto reply = net_->send_append_entries(id_, peer, args);
          if (reply) on_append_entries_reply(peer, args, *reply);
        });
      }
    }
  }   // mu_ goes out of scope here on purpose: the workers' reply handlers take
      // it, so joining while still holding it would deadlock.
  for (auto& w : workers) w.join();
}

int RaftNode::commit_index() const {
  std::lock_guard<std::mutex> g(mu_);
  return commit_index_;
}

int RaftNode::snapshot_index() const {
  std::lock_guard<std::mutex> g(mu_);
  return snapshot_index_;
}

std::vector<std::string> RaftNode::log_contents() const {
  std::lock_guard<std::mutex> g(mu_);
  std::vector<std::string> out;
  out.reserve(log_.size());
  for (const auto& entry : log_)
    out.push_back(std::to_string(entry.term) + ":" + entry.command);
  return out;
}

// Block until entry `index` is applied, then hand back its apply() result (consumed
// once). nullopt on timeout, shutdown, or a term mismatch — the entry applied at
// `index` is not the one propose() placed there, i.e. another leader overwrote it
// and the proposal never committed (Raft §8's check before answering a client).
std::optional<std::string> RaftNode::await_applied(int index, int term,
                                                   milliseconds timeout) {
  std::unique_lock<std::mutex> lk(mu_);
  if (!cv_.wait_for(lk, timeout, [&] { return last_applied_ >= index || !running_; }))
    return std::nullopt;
  auto it = apply_results_.find(index);
  if (it == apply_results_.end()) return std::nullopt;
  // Someone else's entry landed at this index: don't consume its result (its
  // proposer awaits it on another node; here the pruning below reclaims it).
  if (it->second.first != term) return std::nullopt;
  std::string r = std::move(it->second.second);
  apply_results_.erase(it);            // bound the map: each result consumed once
  return r;
}

int RaftNode::leader_hint() const {
  std::lock_guard<std::mutex> g(mu_);
  return leader_id_;
}

// Apply committed-but-unapplied entries to the state machine, in index order.
// Precondition: mu_ held.
void RaftNode::apply_committed() {
  while (last_applied_ < commit_index_) {
    ++last_applied_;
    const LogEntry& entry = log_[last_applied_ - first_index()];
    std::string result = state_machine_->apply(entry.command);
    apply_results_[last_applied_] = {entry.term, std::move(result)};
    cv_.notify_all();   // wake any await_applied waiter
  }
  maybe_compact();
  // Prune apply results one compaction generation behind the snapshot, so
  // followers — which never consume results — stay bounded at ~2 generations.
  // Safe while a single apply_committed advances <= compact_threshold_
  // indices: a live await_applied waiter re-acquires mu_ right after this
  // section and still finds its result. A burst larger than compact_threshold_
  // committed in one step can prune a leader-side waiter's oldest result early
  // -> it degrades to nullopt/timeout (state machine still advanced; never a
  // wrong value).
  std::erase_if(apply_results_, [&](const auto& kv) {
    return kv.first <= snapshot_index_ - compact_threshold_;
  });
}

// Fold the applied prefix into a state-machine snapshot once it grows past the
// threshold: capture state_machine_->snapshot(), drop log_ entries <= last_applied_,
// record the new snapshot boundary, persist. The boundary is last_applied_, never
// commit_index_: a snapshot is the state machine's state, so it can only cover
// entries already applied to it. Precondition: mu_ held.
void RaftNode::maybe_compact() {
  if (last_applied_ - snapshot_index_ <= compact_threshold_) return;
  int new_idx = last_applied_;
  int new_term = term_at(new_idx);   // read before the erase: afterwards the offset no longer points at this entry
  snapshot_ = state_machine_->snapshot();
  log_.erase(log_.begin(), log_.begin() + (new_idx - snapshot_index_));   // drop covered prefix
  snapshot_index_ = new_idx;
  snapshot_term_ = new_term;
  persist();
}

// Save the durable state (term/vote/log + snapshot) as one atomic unit. Precondition: mu_
// held. Idempotent; no-op without storage.
void RaftNode::persist() {
  if (storage_) storage_->save(PersistentState{current_term_, voted_for_, log_,
                                               snapshot_index_, snapshot_term_, snapshot_});
}

}  // namespace nanoraft
