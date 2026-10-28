// YOUR FILE — every `/* Your code here */` below is one function to write; this
// is what you submit. Each one's spec is the doc comment directly above it, and
// `return {}` keeps the skeleton compiling until you replace it. The order here
// is the order to implement: ① election, ② replication, ③ persistence,
// ④ snapshots (see docs/lab3/lab3-tasks.md for the task order).
//
// The scaffolding you call into — random_timeout, the log-offset helpers
// (first_index / last_log_index / term_at / has_entry), apply_committed,
// maybe_compact, persist, recover_from_storage, await_applied, and
// send_heartbeats (the leader's replication round, which builds both requests
// itself and hands each reply to one of the functions below) — is provided in
// src/raft_scaffold.cc. Read it first;
// don't edit it.
//
// The one hard invariant: never hold mu_ across a net_->send_* call. Build the
// args under the lock, release, send from a short-lived thread, re-acquire to
// fold the reply in — and re-check role_/current_term_, the world moved meanwhile.
#include "nanoraft/raft.h"
#include <algorithm>

namespace nanoraft {

using namespace std::chrono;

// Candidate path (Raft §5.2, Fig. 2): increase the currentTerm, become Candidate, vote for
// self (and persist the term/vote), then RequestVote every peer concurrently. Never
// hold mu_ across net_->send_*. If a reply carries a higher term, adopt it and revert
// to Follower.
// On a majority of granted votes (while still Candidate in the same term), take
// leadership: role_ = Leader and initialize the per-peer replication cursors
// (Fig. 2) — nextIndex = last log index + 1 for all, matchIndex = 0 (self = own
// last index). The timer thread picks it up from there and sends the heartbeats.
void RaftNode::start_election() {
  /* Your code here */
}

// Grant a vote if the candidate's term is current+ and we haven't voted for
// someone else this term, and the candidate's log is at least as up-to-date.
RequestVoteReply RaftNode::handle_request_vote(const RequestVoteArgs& args) {
  /* Your code here */
  return {};
}

// Append a client command to the leader's log for later replication (the next
// heartbeat tick ships it). On a non-leader, append nothing and return
// {0, current_term_, false}. Otherwise append at last_log_index()+1, keep
// match_index_[self] in step (so the leader counts toward its own commit majority),
// persist the grown log, then advance_commit_index() — with n_ == 1 the leader
// alone is the majority, and no reply will ever arrive to trigger it — and
// return {index, current_term_, true}.
RaftNode::ProposeResult RaftNode::propose(const std::string& command) {
  /* Your code here */
  return {};
}

// Leader side: a peer answered an AppendEntries we sent. Fold the reply into the
// replication cursors (Fig. 2, §5.3). Note this runs on the *leader*, not the peer. 
// `args` is the request this reply answers — you need the term you sent and how far
// the batch reached. Note to hold the mu_ . 
//   - reply.term > current_term_ → adopt it, revert to Follower, persist, done;
//   - otherwise proceed only while you are still the Leader of args.term (the term
//     you sent in) — a reply that crossed an election is stale;
//   - success → match_index_[peer] = prev_log_index + entries.size() (never
//     backwards — replies can arrive out of order), next_index_ = match + 1, then
//     advance_commit_index();
//   - failure → the consistency check missed: decrement next_index_[peer] (never
//     below 1) and let the next heartbeat tick retry one entry further back
//     (§5.3). A duplicated or reordered failure reply can back you off one extra
//     index; that costs a round trip, never correctness.
// reply.conflict_term / conflict_index (filled in by handle_append_entries, if you
// choose to) are an optional, ungraded speedup — see AppendEntriesReply. The plain
// decrement above is the graded path.
void RaftNode::on_append_entries_reply(int peer, const AppendEntriesArgs& args,
                                       const AppendEntriesReply& reply) {
  /* Your code here */
}

// Leader: raise commit_index_ to the highest N (> commit_index_) that a majority
// has replicated AND that was created in the current term (Fig. 2 / §5.4.2 — a
// leader never commits a prior-term entry by count alone). Precondition: mu_ held.
void RaftNode::advance_commit_index() {
  /* Your code here */
}

// AppendEntries receiver (Fig. 2). Reject stale terms. For a current- or higher-term
// leader, first record the contact in last_heard_ (postpones our election timer) and
// leader_id_ (for client redirect), even if the check below fails; then run the
// log-consistency check at prev_log_index, truncate any conflicting suffix, append
// new entries, then advance the commit index from leaderCommit and apply.
AppendEntriesReply RaftNode::handle_append_entries(const AppendEntriesArgs& args) {
  /* Your code here */
  return {};
}

// InstallSnapshot receiver (Fig. 13). Process the term and leader contact first;
// ignore snapshots through last_applied_ to preserve applied state. Otherwise
// restore the snapshot, retain a tail only if its boundary matches, advance
// commit/applied, and persist.
InstallSnapshotReply RaftNode::handle_install_snapshot(const InstallSnapshotArgs& args) {
  /* Your code here */
  return {};
}

// Leader side: a peer answered an InstallSnapshot we sent. 
// a peer that processed the RPC now holds the whole prefix, so the cursors jump
// straight to last_included_index.
//   - reply.term > current_term_ → adopt it, revert to Follower, persist, done;
//   - otherwise proceed only while you are still the Leader of args.term;
//   - match_index_[peer] = args.last_included_index (never backwards — an
//     AppendEntries reply may already have carried it further), next_index_ =
//     match + 1.
// Unlike on_append_entries_reply, no advance_commit_index() here: the snapshot
// only holds entries the leader had already applied — hence already committed —
// so match_index_ jumping to last_included_index can never pass commit_index_
// and can never unlock a new commit.
void RaftNode::on_install_snapshot_reply(int peer, const InstallSnapshotArgs& args,
                                         const InstallSnapshotReply& reply) {
  /* Your code here */
}

}  // namespace nanoraft
