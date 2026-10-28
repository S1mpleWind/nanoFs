#include "raft_test_group.h"
#include <gtest/gtest.h>
#include <chrono>
#include <random>
#include <string>
#include <thread>
#include <utility>
#include <vector>

using namespace nanoraft;
using namespace std::chrono_literals;

static AppendEntriesArgs make_append_entries(
    int term, int previous_index, int previous_term,
    std::vector<LogEntry> entries, int leader_commit) {
  return AppendEntriesArgs{
      .term = term,
      .leader_id = 1,
      .prev_log_index = previous_index,
      .prev_log_term = previous_term,
      .entries = std::move(entries),
      .leader_commit = leader_commit,
  };
}

// White-box (no cluster; we call one node's RPC handlers directly): destroy
// the node object and rebuild it from the same storage — currentTerm,
// votedFor, and the log must come back exactly. This is the premise every
// restart scenario below builds on.
TEST(RaftPersist, PersistentStateRoundTripsAcrossRestart) {
  MemoryStorage storage;
  MockNetwork network;
  ListStateMachine state_machine;
  {  // this scope is the node's first life: leaving it destroys the node
    RaftNode node(0, 3, &network, &state_machine, &storage);
    auto append_reply = node.handle_append_entries(make_append_entries(
        /*term=*/2, /*previous_index=*/0, /*previous_term=*/0,
        {LogEntry{2, "a"}, LogEntry{2, "b"}}, /*leader_commit=*/0));
    ASSERT_TRUE(append_reply.success);

    auto vote_reply = node.handle_request_vote(RequestVoteArgs{
        .term = 3,
        .candidate_id = 1,
        .last_log_index = 2,
        .last_log_term = 2,
    });
    ASSERT_TRUE(vote_reply.vote_granted);
  }

  PersistentState persisted = storage.load();
  EXPECT_EQ(persisted.current_term, 3);
  EXPECT_EQ(persisted.voted_for, 1);
  ASSERT_EQ(persisted.log.size(), 2u);
  EXPECT_EQ(persisted.log[1].command, "b");

  RaftNode restarted(0, 3, &network, &state_machine, &storage);
  EXPECT_EQ(restarted.current_term(), 3);
}

// The next four tests cover one restart shape each. After every restart, the
// committed history must be found back in storage and the cluster must keep
// committing.

// Every node crashes, so no live copy of the log survives: "first" can only
// come back from storage. Checking that a new command commits is not enough —
// three nodes restarting from empty storage would pass that too — so we also
// check "first" is still at its old index on every node.
TEST(RaftPersist, AllNodesRestartAndRecommit) {
  RaftGroup cluster(3);
  cluster.start_all();
  int first = cluster.propose_until_applied("first", /*applied_on=*/3, 5s);
  ASSERT_GT(first, 0) << "the healthy cluster never committed a first command";

  for (int node = 0; node < 3; ++node) cluster.crash(node);
  for (int node = 0; node < 3; ++node) cluster.restart(node);
  int after = cluster.propose_until_applied("after-full-restart", /*applied_on=*/3, 10s);
  ASSERT_GT(after, 0) << "nobody recovered enough state to elect a leader and commit";
  EXPECT_GT(after, first) << "the recovered log should extend the old one, not replace it";
  EXPECT_EQ(cluster.wait_applied(first, /*applied_on=*/3, 8s), "first")
      << "the committed history did not come back from storage";
}

TEST(RaftPersist, LeaderRestartsAndRecommit) {
  RaftGroup cluster(3);
  cluster.start_all();
  ASSERT_GT(cluster.propose_until_applied("first", /*applied_on=*/3, 5s), 0)
      << "the healthy cluster never committed a first command";

  int leader = cluster.wait_for_one_leader(3s);
  ASSERT_GE(leader, 0) << "no single leader";
  cluster.crash(leader);
  cluster.restart(leader);
  ASSERT_GT(cluster.propose_until_applied("after-leader-restart", /*applied_on=*/3, 10s), 0)
      << "the cluster stopped committing after its leader came back";
}

// The leader stays down while the other two commit an entry; when it rejoins
// it must pick up the entry it missed.
TEST(RaftPersist, LeaderMissesOneCommit) {
  RaftGroup cluster(3);
  cluster.start_all();
  ASSERT_GT(cluster.propose_until_applied("first", /*applied_on=*/3, 5s), 0)
      << "the healthy cluster never committed a first command";

  int down_leader = cluster.wait_for_one_leader(3s);
  ASSERT_GE(down_leader, 0) << "no single leader";
  cluster.crash(down_leader);
  int agreed_while_down =
      cluster.propose_until_applied("leader-down", /*applied_on=*/2, 10s);
  ASSERT_GT(agreed_while_down, 0) << "the two survivors never agreed";
  cluster.restart(down_leader);
  EXPECT_EQ(cluster.wait_applied(agreed_while_down, /*applied_on=*/3, 8s), "leader-down")
      << "the restarted leader never caught up";
}

// Same as LeaderMissesOneCommit, with a follower staying down instead.
TEST(RaftPersist, FollowerMissesOneCommit) {
  RaftGroup cluster(3);
  cluster.start_all();
  ASSERT_GT(cluster.propose_until_applied("first", /*applied_on=*/3, 5s), 0)
      << "the healthy cluster never committed a first command";

  int leader = cluster.wait_for_one_leader(3s);
  ASSERT_GE(leader, 0) << "no single leader";
  int follower = cluster.some_follower(leader);
  cluster.crash(follower);
  int agreed_while_down =
      cluster.propose_until_applied("follower-down", /*applied_on=*/2, 10s);
  ASSERT_GT(agreed_while_down, 0) << "the two survivors never agreed";
  cluster.restart(follower);
  EXPECT_EQ(cluster.wait_applied(agreed_while_down, /*applied_on=*/3, 8s), "follower-down")
      << "the restarted follower never caught up";
  EXPECT_GT(cluster.propose_until_applied("all-back", /*applied_on=*/3, 10s), 0)
      << "the whole cluster stopped agreeing";
}

// Rolling crashes over five nodes. Each round takes two peers down and commits
// with the remaining three — so the two that were down now have stale logs.
// Then those three go down too, and the stale pair comes back with one
// up-to-date node: the election must pick the one recovered log that has the
// newest entries, or the committed entry is lost.
TEST(RaftPersist, MorePersistenceAcrossRollingCrashes) {
  RaftGroup cluster(5);
  cluster.start_all();
  for (int round = 0; round < 3; ++round) {
    SCOPED_TRACE("round " + std::to_string(round));
    std::string tag = "round-" + std::to_string(round) + "-";
    ASSERT_GT(cluster.propose_until_applied(tag + "all-up", /*applied_on=*/5, 20s), 0)
        << "all five never agreed at the start of the round";

    int leader = cluster.wait_for_one_leader(5s);
    ASSERT_GE(leader, 0) << "no single leader";
    std::vector<int> peers = cluster.other_nodes(leader);
    int stale_a = peers[0], stale_b = peers[1];
    int up_to_date = peers[2], spare = peers[3];
    cluster.crash(stale_a);
    cluster.crash(stale_b);
    ASSERT_GT(cluster.propose_until_applied(tag + "three-up", /*applied_on=*/3, 20s), 0)
        << "the remaining three never agreed";
    // Now only leader / up_to_date / spare hold "three-up" in their logs.

    // All three holders of the newest log go down, and the stale pair comes
    // back first: two of five is no majority, so no one can get elected...
    cluster.crash(leader);
    cluster.crash(up_to_date);
    cluster.crash(spare);
    cluster.restart(stale_a);
    cluster.restart(stale_b);
    std::this_thread::sleep_for(600ms);            // one full election window
    // ...until up_to_date rejoins: the one newest log among the three running
    // nodes must win the election and bring the stale pair up to date.
    cluster.restart(up_to_date);
    ASSERT_GT(cluster.propose_until_applied(tag + "recovered", /*applied_on=*/3, 20s), 0)
        << "the three restarted nodes never agreed — the stale pair may have out-voted "
           "the only up-to-date log";

    cluster.restart(spare);
    cluster.restart(leader);
  }
  ASSERT_GT(cluster.propose_until_applied("final", /*applied_on=*/5, 20s), 0)
      << "all five never agreed after the rounds finished";
}

// Twelve rounds of churn: keep proposing while leaders crash. A leader that
// crashes mid-replication leaves its last entries on some nodes, uncommitted.
// The next leader must finish spreading those older-term entries but must NOT
// commit them just because they reach a majority — they commit only behind an
// entry of the new leader's own term (§5.4.2 / Fig. 8). Committing too early
// makes nodes apply different commands at the same index — n_applied() reports
// that and wait_applied() fails. A majority stays alive throughout; at the end
// every node comes back and must agree on one history.
TEST(RaftPersist, Figure8LeaderCrashLoop) {
  std::mt19937 random(7);
  RaftGroup cluster(5);
  cluster.start_all();
  std::vector<bool> running(5, true);
  int running_count = 5;
  auto crash = [&](int node) {
    cluster.crash(node);
    running[node] = false;
    --running_count;
  };
  auto restart_one_crashed = [&] {
    for (int node = 0; node < 5; ++node) {
      if (!running[node]) {
        cluster.restart(node);
        running[node] = true;
        ++running_count;
        return;
      }
    }
  };

  for (int round = 1; round <= 12; ++round) {
    SCOPED_TRACE("round " + std::to_string(round));
    std::string command = "churn-" + std::to_string(round);
    bool crash_mid_replication = (round % 2 == 0);
    if (crash_mid_replication) {
      // Offer the command to whichever node will take it, then crash that
      // leader before waiting for any commit — this is how those uncommitted
      // older-term entries get created.
      int acceptor = -1;
      for (int node = 0; node < 5; ++node) {
        if (running[node] && cluster.node(node)->propose(command).is_leader)
          acceptor = node;
      }
      if (acceptor >= 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(random() % 13));
        crash(acceptor);
      }
    } else {
      // Wait for the command to commit — the churning cluster must still make
      // progress — then sometimes crash the leader anyway.
      ASSERT_GT(cluster.propose_until_applied(command, /*applied_on=*/3, 30s), 0)
          << command << " never agreed (-1 = timeout, or nodes applied different commands)";
      auto leaders = cluster.leaders();
      if (leaders.size() == 1 && (random() % 2 == 0)) crash(leaders[0]);
    }
    // Keep a majority alive: whenever fewer than 4 run, bring one back.
    if (running_count < 4) restart_one_crashed();
  }

  for (int node = 0; node < 5; ++node) {
    if (!running[node]) cluster.restart(node);
  }
  ASSERT_GT(cluster.propose_until_applied("final", /*applied_on=*/5, 30s), 0)
      << "the five nodes never converged on one history";
}
