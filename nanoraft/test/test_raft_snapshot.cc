#include "raft_test_group.h"
#include <gtest/gtest.h>
#include <chrono>
#include <string>
#include <utility>
#include <vector>

using namespace nanoraft;
using namespace std::chrono_literals;

// §2.4 has two layers of tests in this file:
//   1. RaftInstallSnapshot.* drives one unstarted follower directly
//   2. RaftSnapshot.* uses a running cluster and mirrors MIT 6.824's
//      disconnected-install, unreliable-install, and crash/restart setups.

// Builds the AppendEntries RPC sent by `leader_id`. Entries default to empty
// (a heartbeat), leader_commit to 0. The receiver tests seed and probe the
// follower's log with these through the §2.2 receiver, so get
// `RaftAppendEntries` green first.
static AppendEntriesArgs make_append_entries(int term, int leader_id,
                                             int prev_log_index,
                                             int prev_log_term,
                                             std::vector<LogEntry> entries = {},
                                             int leader_commit = 0) {
  return AppendEntriesArgs{
      .term = term,
      .leader_id = leader_id,
      .prev_log_index = prev_log_index,
      .prev_log_term = prev_log_term,
      .entries = std::move(entries),
      .leader_commit = leader_commit,
  };
}

// Builds the InstallSnapshot RPC sent by `leader_id`, carrying `data` as the
// snapshot bytes.
static InstallSnapshotArgs make_install_snapshot(int term, int leader_id,
                                                 int last_included_index,
                                                 int last_included_term,
                                                 std::string data) {
  return InstallSnapshotArgs{
      .term = term,
      .leader_id = leader_id,
      .last_included_index = last_included_index,
      .last_included_term = last_included_term,
      .data = std::move(data),
  };
}

// Snapshot bytes holding exactly `commands`, in ListStateMachine's format.
static std::string snapshot_of(const std::vector<std::string>& commands) {
  ListStateMachine donor;
  for (const auto& command : commands) donor.apply(command);
  return donor.snapshot();
}

// A stale leader's snapshot is rejected: the reply carries the newer
// term and nothing on the follower changes.
TEST(RaftInstallSnapshot, RejectsStaleTermWithoutStateChange) {
  ListStateMachine state_machine;
  MockNetwork network;
  // Node 0 of a 3-node cluster receives every RPC below. It is never started
  // (no timer thread), so it stays a follower. Node 1 is the live term-2
  // leader; node 2 is a deposed term-1 leader that does not yet know it lost.
  RaftNode follower(0, 3, &network, &state_machine);

  // Leader 1 replicates one entry. Follower state now: term 2, no snapshot,
  // nothing applied, log:
  //   index 1 -> term 2, "a"
  ASSERT_TRUE(follower
                  .handle_append_entries(make_append_entries(
                      /*term=*/2, /*leader_id=*/1, /*prev_log_index=*/0,
                      /*prev_log_term=*/0, {LogEntry{2, "a"}}))
                  .success);

  // The deposed node 2 offers a term-1 snapshot through index 5.
  auto stale_reply = follower.handle_install_snapshot(make_install_snapshot(
      /*term=*/1, /*leader_id=*/2, /*last_included_index=*/5,
      /*last_included_term=*/1, snapshot_of({"x"})));

  EXPECT_EQ(stale_reply.term, 2);
  EXPECT_EQ(follower.snapshot_index(), 0);
  EXPECT_TRUE(state_machine.applied().empty());
  // The log survived: the entry at index 1 still matches term 2.
  EXPECT_TRUE(follower
                  .handle_append_entries(make_append_entries(
                      /*term=*/2, /*leader_id=*/1, /*prev_log_index=*/1,
                      /*prev_log_term=*/2))
                  .success);
}

// The basic install: the snapshot replaces the whole log, the state machine is
// restored from its bytes, and commit/applied jump to last_included_index. A
// retransmitted, already-covered snapshot must then be a no-op.
TEST(RaftInstallSnapshot, InstallReplacesLogRestoresStateMachineAndAdvancesCommit) {
  ListStateMachine state_machine;
  MockNetwork network;
  // Node 0 of a 3-node cluster, unstarted so it stays a follower; node 1 is
  // the leader sending every RPC below.
  RaftNode follower(0, 3, &network, &state_machine);

  // Leader 1 at term 1 replicates two entries. Follower state now: term 1,
  // nothing committed or applied, log:
  //   index 1 -> term 1, "a"
  //   index 2 -> term 1, "b"
  ASSERT_TRUE(follower
                  .handle_append_entries(make_append_entries(
                      /*term=*/1, /*leader_id=*/1, /*prev_log_index=*/0,
                      /*prev_log_term=*/0, {LogEntry{1, "a"}, LogEntry{1, "b"}}))
                  .success);

  // Node 1 leads term 3 by now and has compacted past this whole log: its
  // snapshot holds s1..s5 and ends at (index 5, term 3).
  auto reply = follower.handle_install_snapshot(make_install_snapshot(
      /*term=*/3, /*leader_id=*/1, /*last_included_index=*/5,
      /*last_included_term=*/3, snapshot_of({"s1", "s2", "s3", "s4", "s5"})));

  EXPECT_EQ(reply.term, 3);
  EXPECT_EQ(follower.snapshot_index(), 5);
  EXPECT_EQ(follower.commit_index(), 5);
  EXPECT_EQ(state_machine.applied(),
            (std::vector<std::string>{"s1", "s2", "s3", "s4", "s5"}));

  // The offset log continues right after the snapshot: index 6 appends and
  // commits on top of the restored prefix.
  ASSERT_TRUE(follower
                  .handle_append_entries(make_append_entries(
                      /*term=*/3, /*leader_id=*/1, /*prev_log_index=*/5,
                      /*prev_log_term=*/3, {LogEntry{3, "tail"}},
                      /*leader_commit=*/6))
                  .success);
  EXPECT_EQ(state_machine.applied().back(), "tail");

  // A duplicate of the now-covered snapshot must not roll the follower back.
  follower.handle_install_snapshot(make_install_snapshot(
      /*term=*/3, /*leader_id=*/1, /*last_included_index=*/5,
      /*last_included_term=*/3, snapshot_of({"stale"})));
  EXPECT_EQ(follower.snapshot_index(), 5);
  EXPECT_EQ(state_machine.applied().back(), "tail");
}

// Entries strictly beyond last_included_index
// survive iff the entry at the boundary carries the matching term. 
TEST(RaftInstallSnapshot, KeepsTailBeyondSnapshotWhenBoundaryTermMatches) {
  ListStateMachine state_machine;
  MockNetwork network;
  // Node 0 of a 3-node cluster, unstarted so it stays a follower; node 1 is
  // the leader sending every RPC below.
  RaftNode follower(0, 3, &network, &state_machine);

  // Leader 1 at term 1 replicates three entries. Follower state now: term 1,
  // nothing committed or applied, log:
  //   index 1 -> term 1, "a"
  //   index 2 -> term 1, "b"
  //   index 3 -> term 1, "c"
  ASSERT_TRUE(follower
                  .handle_append_entries(make_append_entries(
                      /*term=*/1, /*leader_id=*/1, /*prev_log_index=*/0,
                      /*prev_log_term=*/0,
                      {LogEntry{1, "a"}, LogEntry{1, "b"}, LogEntry{1, "c"}}))
                  .success);

  // Node 1 (term 2 now) sends a snapshot ending at (index 2, term 1) — and the
  // follower's own index 2 also carries term 1, so the boundary matches: "c"
  // at index 3 must survive.
  auto reply = follower.handle_install_snapshot(make_install_snapshot(
      /*term=*/2, /*leader_id=*/1, /*last_included_index=*/2,
      /*last_included_term=*/1, snapshot_of({"a", "b"})));

  EXPECT_EQ(reply.term, 2);
  EXPECT_EQ(follower.snapshot_index(), 2);
  EXPECT_EQ(state_machine.applied(), (std::vector<std::string>{"a", "b"}));

  // "c" at index 3 survived: a matching heartbeat can still commit it.
  ASSERT_TRUE(follower
                  .handle_append_entries(make_append_entries(
                      /*term=*/2, /*leader_id=*/1, /*prev_log_index=*/3,
                      /*prev_log_term=*/1, {}, /*leader_commit=*/3))
                  .success);
  EXPECT_EQ(state_machine.applied(),
            (std::vector<std::string>{"a", "b", "c"}));
}

// When the follower's entry at last_included_index carries a different term
// than the snapshot, the follower discards its entire log (§7): everything up
// to last_included_index is replaced by the snapshot, and everything after it
// builds on the mismatched entry — a different history, so it must go too.
TEST(RaftInstallSnapshot, ClearsWholeLogWhenBoundaryTermMismatches) {
  ListStateMachine state_machine;
  MockNetwork network;
  // Node 0 of a 3-node cluster, unstarted so it stays a follower. Node 1 led
  // term 1; node 2 leads term 2 with a history that diverged from node 1's.
  RaftNode follower(0, 3, &network, &state_machine);

  // The old leader 1 replicated three term-1 entries. Follower state now:
  // term 1, nothing committed or applied, log:
  //   index 1 -> term 1, "a"
  //   index 2 -> term 1, "b"
  //   index 3 -> term 1, "c"
  ASSERT_TRUE(follower
                  .handle_append_entries(make_append_entries(
                      /*term=*/1, /*leader_id=*/1, /*prev_log_index=*/0,
                      /*prev_log_term=*/0,
                      {LogEntry{1, "a"}, LogEntry{1, "b"}, LogEntry{1, "c"}}))
                  .success);

  // Leader 2's snapshot ends at (index 2, term 2), but the follower's index 2
  // carries term 1 — a different history, so nothing after the boundary can be
  // trusted.
  follower.handle_install_snapshot(make_install_snapshot(
      /*term=*/2, /*leader_id=*/2, /*last_included_index=*/2,
      /*last_included_term=*/2, snapshot_of({"p", "q"})));

  EXPECT_EQ(follower.snapshot_index(), 2);
  EXPECT_EQ(state_machine.applied(), (std::vector<std::string>{"p", "q"}));
  // The old index 3 is gone: the log now ends at the snapshot, so an append that
  // attaches after index 3 no longer has anything to attach to.
  auto probe = follower.handle_append_entries(make_append_entries(
      /*term=*/2, /*leader_id=*/2, /*prev_log_index=*/3, /*prev_log_term=*/1));
  EXPECT_FALSE(probe.success);
}

// Old snapshots must preserve applied state, including at the equal-index boundary.
TEST(RaftInstallSnapshot, IgnoresSnapshotAtOrBelowLastApplied) {
  const std::vector<std::string> commands{"a", "b", "c"};
  for (int included_index : {2, 3}) {
    SCOPED_TRACE(included_index);
    ListStateMachine state_machine;
    MockNetwork network;
    RaftNode follower(0, 3, &network, &state_machine);

    ASSERT_TRUE(follower
                    .handle_append_entries(make_append_entries(
                        /*term=*/1, /*leader_id=*/1, /*prev_log_index=*/0,
                        /*prev_log_term=*/0,
                        {LogEntry{1, "a"}, LogEntry{1, "b"}, LogEntry{1, "c"}},
                        /*leader_commit=*/3))
                    .success);
    ASSERT_EQ(state_machine.applied(), commands);
    const auto log_before = follower.log_contents();

    const std::vector<std::string> prefix(commands.begin(),
                                          commands.begin() + included_index);
    follower.handle_install_snapshot(make_install_snapshot(
        /*term=*/1, /*leader_id=*/1, included_index,
        /*last_included_term=*/1, snapshot_of(prefix)));
    EXPECT_EQ(follower.snapshot_index(), 0);
    EXPECT_EQ(follower.commit_index(), 3);
    EXPECT_EQ(follower.log_contents(), log_before);
    EXPECT_EQ(state_machine.applied(), commands);

    // An unchanged commit index would leave a rolled-back state machine stuck.
    ASSERT_TRUE(follower
                    .handle_append_entries(make_append_entries(
                        /*term=*/1, /*leader_id=*/1, /*prev_log_index=*/3,
                        /*prev_log_term=*/1, {}, /*leader_commit=*/3))
                    .success);
    EXPECT_EQ(follower.commit_index(), 3);
    EXPECT_EQ(state_machine.applied(), commands);
  }
}

// Ignoring snapshot data must not skip adopting and persisting a higher term.
TEST(RaftInstallSnapshot, OldSnapshotStillUpdatesTermAndLeader) {
  ListStateMachine state_machine;
  MockNetwork network;
  MemoryStorage storage;
  RaftNode follower(0, 3, &network, &state_machine, &storage);
  ASSERT_TRUE(follower.handle_request_vote(RequestVoteArgs{
      .term = 1, .candidate_id = 1}).vote_granted);

  ASSERT_TRUE(follower
                  .handle_append_entries(make_append_entries(
                      /*term=*/1, /*leader_id=*/1, /*prev_log_index=*/0,
                      /*prev_log_term=*/0,
                      {LogEntry{1, "a"}, LogEntry{1, "b"}, LogEntry{1, "c"}},
                      /*leader_commit=*/3))
                  .success);
  ASSERT_EQ(state_machine.applied(), (std::vector<std::string>{"a", "b", "c"}));

  auto reply = follower.handle_install_snapshot(make_install_snapshot(
      /*term=*/2, /*leader_id=*/2, /*last_included_index=*/2,
      /*last_included_term=*/1, snapshot_of({"a", "b"})));
  EXPECT_EQ(reply.term, 2);
  EXPECT_EQ(follower.current_term(), 2);
  EXPECT_EQ(follower.leader_hint(), 2);
  EXPECT_EQ(follower.snapshot_index(), 0);
  EXPECT_EQ(follower.commit_index(), 3);
  EXPECT_EQ(state_machine.applied(), (std::vector<std::string>{"a", "b", "c"}));
  const auto saved = storage.load();
  EXPECT_EQ(saved.current_term, 2);
  EXPECT_EQ(saved.voted_for, -1);
  EXPECT_EQ(saved.snapshot_index, 0);
  EXPECT_EQ(saved.log.size(), 3u);
}

// MIT 6.824 TestSnapshotInstall: a follower that misses the compacted prefix
// catches up through InstallSnapshot after reconnecting.
TEST(RaftSnapshot, InstallsSnapshotAfterFollowerDisconnect) {
  RaftGroup cluster(3, /*compact_threshold=*/8);
  cluster.start_all();
  int leader = cluster.wait_for_one_leader(2s);
  ASSERT_GE(leader, 0);
  int lagging_follower = cluster.some_follower(leader);
  cluster.disconnect(lagging_follower);

  // The connected pair is a majority: it commits 20 commands, and threshold 8
  // guarantees both compacted along the way. Cluster state after the loop:
  // leader + one follower applied through index 20 with the early log entries
  // gone, the disconnected follower still empty at index 0.
  for (int command_number = 1; command_number <= 20; ++command_number) {
    ASSERT_GE(cluster.propose_to_leader(
                  "command-" + std::to_string(command_number)),
              1);
  }
  EXPECT_EQ(cluster.wait_applied(20, /*applied_on=*/2, 3s), "command-20");
  EXPECT_GT(cluster.snapshot_index(leader), 0);

  // The next entry the follower needs no longer exists in the leader's log,
  // so only InstallSnapshot can catch it up.
  cluster.reconnect(lagging_follower);
  EXPECT_EQ(cluster.wait_applied(20, /*applied_on=*/3, 4s), "command-20");
}

// MIT 6.824 TestSnapshotInstallUnreliable: snapshot catch-up must tolerate lost
// requests and replies, not only a clean reconnect.
TEST(RaftSnapshot, InstallsSnapshotAfterDisconnectWithMessageLoss) {
  RaftGroup cluster(3, /*compact_threshold=*/8);
  cluster.start_all();
  int leader = cluster.wait_for_one_leader(2s);
  ASSERT_GE(leader, 0);
  int lagging_follower = cluster.some_follower(leader);
  cluster.disconnect(lagging_follower);

  // Same setup as the test above: the connected pair commits 20 commands and
  // compacts, while the disconnected follower stays empty at index 0.
  for (int command_number = 1; command_number <= 20; ++command_number) {
    ASSERT_GE(cluster.propose_to_leader(
                  "unreliable-" + std::to_string(command_number)),
              1);
  }
  ASSERT_EQ(cluster.wait_applied(20, /*applied_on=*/2, 3s), "unreliable-20");
  ASSERT_GT(cluster.snapshot_index(leader), 0);

  // Catch-up now runs with every RPC having a 15% chance of being dropped:
  // a lost InstallSnapshot request or reply must be retried until it lands.
  cluster.net()->set_loss(0.15, 84);
  cluster.reconnect(lagging_follower);
  EXPECT_EQ(cluster.wait_applied(20, /*applied_on=*/3, 10s), "unreliable-20");
}

// A snapshot survives crash+restart: a restarted node restores its SM from the
// persisted snapshot instead of replaying a truncated log.
TEST(RaftSnapshot, RestoresPersistedSnapshotAfterRestart) {
  RaftGroup cluster(3, /*compact_threshold=*/8);
  cluster.start_all();
  int leader = cluster.wait_for_one_leader(2s);
  ASSERT_GE(leader, 0);
  // All three nodes apply 20 commands; with threshold 8 each has compacted,
  // so part of every node's history now lives only in its snapshot.
  for (int command_number = 1; command_number <= 20; ++command_number) {
    ASSERT_GE(cluster.propose_to_leader(
                  "persistent-" + std::to_string(command_number)),
              1);
  }
  ASSERT_EQ(cluster.wait_applied(20, /*applied_on=*/3, 3s), "persistent-20");

  // Crash and restart one follower that has compacted: recovery must load the
  // persisted snapshot — the entries it covers are no longer in the persisted
  // log to replay.
  int follower = cluster.some_follower(leader);
  ASSERT_GT(cluster.snapshot_index(follower), 0);
  cluster.crash(follower);
  cluster.restart(follower);

  EXPECT_EQ(cluster.wait_applied(20, /*applied_on=*/3, 3s), "persistent-20");
}

// Compaction keeps running (6.824 bounds the whole Raft state with MAXLOGSIZE;
// here the snapshot may trail the 20 applied entries by at most one threshold
// plus the batch in flight, so an implementation that compacts once and stops
// sticks out).
TEST(RaftSnapshot, CompactionKeepsRunning) {
  RaftGroup cluster(3, /*compact_threshold=*/4);
  cluster.start_all();
  ASSERT_GE(cluster.wait_for_one_leader(3s), 0);
  // A healthy 3-node cluster commits 20 commands one by one, each applied on
  // all three nodes before the next, so every node's last_applied_ marches to
  // 20 together.
  for (int command_number = 1; command_number <= 20; ++command_number) {
    ASSERT_GT(cluster.propose_until_applied(
                  "bounded-" + std::to_string(command_number), /*applied_on=*/3, 15s),
              0);
  }
  for (int node = 0; node < 3; ++node) {
    EXPECT_GE(cluster.snapshot_index(node), 12)
        << "node " << node << " stopped compacting";
  }
}
