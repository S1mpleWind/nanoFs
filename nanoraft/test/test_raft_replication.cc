#include "raft_test_group.h"
#include <gtest/gtest.h>
#include <chrono>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <utility>
#include <vector>

using namespace nanoraft;
using namespace std::chrono_literals;

// §2.2's tests come in two layers, in the order you should implement against:
//   1. RaftAppendEntries.* calls handle_append_entries() on one node directly.
//      No threads, no timing — one Figure 2 receiver rule per case.
//   2. RaftReplication.* runs a real cluster, so it needs all five functions.
//      Each case replays a scenario from MIT 6.824's Raft lab test suite, with
//      fewer commands so the suite takes minutes, not hours.

class RaftAppendEntries : public ::testing::Test {
 protected:
  // Deliver one AppendEntries from leader node 1 to the follower and return
  // its reply. Entries default to empty (a heartbeat), leader_commit to 0.
  AppendEntriesReply receive(
      int term, int prev_log_index, int prev_log_term,
      std::vector<LogEntry> entries = {}, int leader_commit = 0) {
    return follower_.handle_append_entries(AppendEntriesArgs{
        .term = term,
        .leader_id = 1,
        .prev_log_index = prev_log_index,
        .prev_log_term = prev_log_term,
        .entries = std::move(entries),
        .leader_commit = leader_commit,
    });
  }

  MockNetwork network_;
  ListStateMachine state_machine_;
  RaftNode follower_{0, 3, &network_, &state_machine_};
};

// Figure 2 rule 1: reject an older leader without changing the follower's log.
// The stale request attaches at 0/0, so its consistency check would pass —
// only the term check can reject it.
TEST_F(RaftAppendEntries, RejectsStaleTermWithoutChangingLog) {
  ASSERT_TRUE(receive(/*term=*/2, /*prev_log_index=*/0, /*prev_log_term=*/0,
                      {LogEntry{2, "a"}})
                  .success);

  auto stale_reply = receive(/*term=*/1, /*prev_log_index=*/0,
                             /*prev_log_term=*/0, {LogEntry{1, "stale"}});

  EXPECT_FALSE(stale_reply.success);
  EXPECT_EQ(stale_reply.term, 2);
  EXPECT_EQ(follower_.log_contents(), (std::vector<std::string>{"2:a"}))
      << "\"stale\" got appended; the term check must come before any append";
  EXPECT_TRUE(state_machine_.applied().empty());
}

// Figure 2 rule 2, first way to fail it: the follower's log does not even reach
// prev_log_index. Nothing may be appended past a gap.
TEST_F(RaftAppendEntries, RejectsPreviousEntryBeyondEndOfLog) {
  ASSERT_TRUE(receive(/*term=*/1, /*prev_log_index=*/0, /*prev_log_term=*/0,
                      {LogEntry{1, "a"}})
                  .success);

  auto reply = receive(/*term=*/1, /*prev_log_index=*/5, /*prev_log_term=*/1,
                       {LogEntry{1, "z"}});

  EXPECT_FALSE(reply.success);
  EXPECT_EQ(follower_.log_contents(), (std::vector<std::string>{"1:a"}))
      << "\"z\" must not land at index 6 with 2..5 missing";
}

// Figure 2 rule 2, second way to fail it: prev_log_index exists but carries a
// different term. Here a newly elected term-3 leader, whose own index 3 holds a
// term-3 entry, sends its first heartbeat. The leader will retry one index
// further back.
TEST_F(RaftAppendEntries, RejectsMismatchedPreviousTerm) {
  ASSERT_TRUE(receive(/*term=*/2, /*prev_log_index=*/0, /*prev_log_term=*/0,
                      {LogEntry{1, "a"}, LogEntry{2, "b"},
                       LogEntry{2, "c"}})
                  .success);

  auto mismatch = receive(/*term=*/3, /*prev_log_index=*/3, /*prev_log_term=*/3);

  EXPECT_FALSE(mismatch.success);
  EXPECT_EQ(follower_.log_contents(),
            (std::vector<std::string>{"1:a", "2:b", "2:c"}))
      << "a rejected request must leave the log intact — truncation happens only "
         "on the success path";
}

// Figure 2 rule 3: replacing the first conflicting entry removes every entry
// after it, not just the conflicting slot itself.
TEST_F(RaftAppendEntries, TruncatesTheEntireConflictingSuffix) {
  ASSERT_TRUE(receive(/*term=*/1, /*prev_log_index=*/0, /*prev_log_term=*/0,
                      {LogEntry{1, "a"}, LogEntry{1, "b"},
                       LogEntry{1, "orphan"}})
                  .success);

  auto replacement = receive(/*term=*/2, /*prev_log_index=*/1,
                             /*prev_log_term=*/1, {LogEntry{2, "c"}});
  ASSERT_TRUE(replacement.success);

  EXPECT_EQ(follower_.log_contents(), (std::vector<std::string>{"1:a", "2:c"}))
      << "replacing index 2 must drop the orphan at index 3 as well";
}

// Figure 2 rule 4: retransmitting entries already present must not grow the log.
TEST_F(RaftAppendEntries, RetransmissionDoesNotDuplicateEntries) {
  const std::vector<LogEntry> entries{
      LogEntry{1, "a"},
      LogEntry{1, "b"},
  };
  ASSERT_TRUE(receive(/*term=*/1, /*prev_log_index=*/0, /*prev_log_term=*/0,
                      entries)
                  .success);

  ASSERT_TRUE(receive(/*term=*/1, /*prev_log_index=*/0, /*prev_log_term=*/0,
                      entries)
                  .success);

  EXPECT_EQ(follower_.log_contents(), (std::vector<std::string>{"1:a", "1:b"}))
      << "the second copy grew the log; an entry already present stays put";
}

// Empty AppendEntries confirms only its prefix. It must not truncate a longer,
// non-conflicting follower log. (The empty request here is a delayed old
// heartbeat from when the leader's log ended at index 1 — the network may
// delay and reorder, §1.4.)
TEST_F(RaftAppendEntries, MatchingHeartbeatPreservesUnconfirmedSuffix) {
  ASSERT_TRUE(receive(/*term=*/1, /*prev_log_index=*/0, /*prev_log_term=*/0,
                      {LogEntry{1, "a"}, LogEntry{1, "b"},
                       LogEntry{1, "c"}})
                  .success);

  ASSERT_TRUE(receive(/*term=*/1, /*prev_log_index=*/1, /*prev_log_term=*/1,
                      {}, /*leader_commit=*/1)
                  .success);
  EXPECT_EQ(follower_.log_contents(),
            (std::vector<std::string>{"1:a", "1:b", "1:c"}))
      << "an empty AppendEntries confirms a prefix; it must not truncate";
  EXPECT_EQ(state_machine_.applied(),
            (std::vector<std::string>{"a"}));

  // A later heartbeat can still match and commit the preserved suffix.
  ASSERT_TRUE(receive(/*term=*/1, /*prev_log_index=*/3, /*prev_log_term=*/1,
                      {}, /*leader_commit=*/3)
                  .success);
  EXPECT_EQ(state_machine_.applied(),
            (std::vector<std::string>{"a", "b", "c"}));
}

// Wait out more than two election windows, then require that NO node applied
// `index`.
static void expect_no_agreement(RaftGroup& cluster, int index) {
  std::this_thread::sleep_for(1500ms);
  EXPECT_EQ(cluster.n_applied(index).first, 0)
      << "index " << index << " reached a state machine without a majority";
}

// send_heartbeats() must keep running: under a live leader no follower times
// out, so no term ever moves. A cluster quietly re-electing itself every window
// still shows "one leader" at any instant, so the term is what catches it.
TEST(RaftReplication, HeartbeatsKeepTermsStable) {
  RaftGroup cluster(3);
  cluster.start_all();
  int leader = cluster.wait_for_one_leader(2s);
  ASSERT_GE(leader, 0);

  auto current_terms = [&] {
    std::vector<int> terms;
    for (int node = 0; node < cluster.size(); ++node)
      terms.push_back(cluster.node(node)->current_term());
    return terms;
  };
  std::this_thread::sleep_for(300ms);
  std::vector<int> settled_terms(
      cluster.size(), cluster.node(leader)->current_term());
  ASSERT_EQ(current_terms(), settled_terms)
      << "cluster never settled into the leader's term";

  std::this_thread::sleep_for(1500ms);
  EXPECT_EQ(current_terms(), settled_terms)
      << "a term moved under a healthy leader; heartbeats are not landing";
  EXPECT_EQ(cluster.leaders(), std::vector<int>{leader})
      << "leadership changed hands";
}

// A healthy cluster agrees on command after command, and nothing commits
// before it is proposed.
TEST(RaftReplication, BasicAgreement) {
  RaftGroup cluster(3);
  cluster.start_all();
  int leader = cluster.wait_for_one_leader(2s);
  ASSERT_GE(leader, 0);

  // the follower cannot propose 
  EXPECT_FALSE(
      cluster.node(cluster.some_follower(leader))->propose("rejected").is_leader);

  for (int command_number = 1; command_number <= 3; ++command_number) {
    ASSERT_EQ(cluster.n_applied(command_number).first, 0)
        << "index " << command_number << " committed before it was proposed";
    std::string command = "command-" + std::to_string(command_number);
    int index = cluster.propose_to_leader(command);
    ASSERT_EQ(index, command_number) << "(-1 = there was no single leader)";
    EXPECT_EQ(cluster.wait_applied(index, /*applied_on=*/3, 2s), command);
  }
}

// A cluster of one: the leader is its own majority, and no AppendEntries reply
// will ever arrive, so propose() itself must advance the commit index.
TEST(RaftReplication, SingleNodeCommitsWithoutReplies) {
  RaftGroup cluster(1);
  cluster.start_all();
  ASSERT_GE(cluster.wait_for_one_leader(2s), 0);

  int index = cluster.propose_to_leader("alone");
  ASSERT_EQ(index, 1);
  EXPECT_EQ(cluster.wait_applied(index, /*applied_on=*/1, 2s), "alone")
      << "commit_index stayed at " << cluster.node(0)->commit_index()
      << "; with no peers nothing but propose() can advance it";
}

// §5.4.2 / Fig. 8: a leader must never commit an entry from an EARLIER term by
// replica count alone. Such an entry commits only transitively — once an entry
// of the leader's own term reaches a majority, everything before it commits
// too. Setup: nodes 0 and 1 took a term-1 entry from a leader that died before
// committing it, and node 2 is cut off, so those two are the only possible
// majority. Whoever wins gets "old" onto 2 of 3 and must still not commit it.
TEST(RaftReplication, DoesNotCommitPriorTermEntryByCount) {
  RaftGroup cluster(3);
  for (int node : {0, 1}) {
    ASSERT_TRUE(cluster.node(node)
                    ->handle_append_entries(AppendEntriesArgs{
                        .term = 1,
                        .leader_id = 2,
                        .prev_log_index = 0,
                        .prev_log_term = 0,
                        .entries = {LogEntry{1, "old"}},
                        .leader_commit = 0,
                    })
                    .success);
  }
  cluster.disconnect(2);
  cluster.start_all();
  int leader = cluster.wait_for_one_leader(5s);
  ASSERT_GE(leader, 0) << "no single leader";

  // Many heartbeat rounds: long enough for the log entry "old" to sit on both reachable
  // nodes, and for a count-only commit rule to have fired.
  std::this_thread::sleep_for(1s);
  EXPECT_EQ(cluster.node(leader)->commit_index(), 0)
      << "committed a prior-term entry by count alone";
  EXPECT_EQ(cluster.n_applied(1).first, 0)
      << "\"old\" reached a state machine before any current-term entry committed";

  // One entry of the leader's own term, and index 1 commits behind it.
  ASSERT_EQ(cluster.propose_to_leader("new"), 2)
      << "(-1 = there was no single leader)";
  EXPECT_EQ(cluster.wait_applied(2, /*applied_on=*/2, 5s), "new");
  EXPECT_EQ(cluster.n_applied(1).second, "old")
      << "index 1 did not commit transitively behind index 2";
}

// The cluster keeps agreeing with one follower disconnected, and the follower
// participates again after it reconnects.
TEST(RaftReplication, AgreementAfterFollowerReconnects) {
  RaftGroup cluster(3);
  cluster.start_all();
  ASSERT_GT(cluster.propose_until_applied("all-up", /*applied_on=*/3, 5s), 0)
      << "the healthy cluster never committed a first command";

  int leader = cluster.wait_for_one_leader(3s);
  ASSERT_GE(leader, 0) << "no single leader";
  int disconnected_follower = cluster.some_follower(leader);
  cluster.disconnect(disconnected_follower);

  // The leader and the remaining follower are a majority of three.
  int last_index = 0;
  for (int command_number = 1; command_number <= 4; ++command_number) {
    last_index = cluster.propose_until_applied(
        "while-disconnected-" + std::to_string(command_number),
        /*applied_on=*/2, 5s);
    ASSERT_GT(last_index, 0)
        << "command " << command_number << " never committed on the majority";
  }

  // The follower bumped its term while isolated, so rejoining may cost one
  // election before the full set agrees again.
  cluster.reconnect(disconnected_follower);
  EXPECT_EQ(cluster.wait_applied(last_index, /*applied_on=*/3, 8s),
            "while-disconnected-4")
      << "the reconnected follower never caught up";
  EXPECT_GT(cluster.propose_until_applied("after-reconnect", /*applied_on=*/3, 8s), 0)
      << "the full cluster stopped agreeing after the follower rejoined";
}

// A leader disconnected from three of five peers cannot commit. Once they are
// back, whoever wins the election may or may not hold that entry, so the next
// command lands at index 2 or 3 — both are legal.
TEST(RaftReplication, NoAgreementIfTooManyFollowersDisconnect) {
  RaftGroup cluster(5);
  cluster.start_all();
  ASSERT_EQ(cluster.propose_until_applied("base", /*applied_on=*/5, 5s), 1)
      << "the healthy cluster never committed a first command at index 1";

  int leader = cluster.wait_for_one_leader(3s);
  ASSERT_GE(leader, 0) << "no single leader";
  std::vector<int> followers = cluster.other_nodes(leader);
  for (int i = 0; i < 3; ++i) cluster.disconnect(followers[i]);

  auto orphan = cluster.node(leader)->propose("minority-only");
  ASSERT_TRUE(orphan.is_leader) << "the isolated leader rejected the proposal";
  ASSERT_EQ(orphan.index, 2);
  expect_no_agreement(cluster, 2);

  // Reconnect. The three that were disconnected never saw index 2, and they
  // come back with terms they bumped while alone; if one of them wins the
  // election index 2 is overwritten, so the next command lands at index 2 or 3.
  for (int i = 0; i < 3; ++i) cluster.reconnect(followers[i]);
  RaftNode::ProposeResult after_repair{};
  for (auto deadline = std::chrono::steady_clock::now() + 10s;
       std::chrono::steady_clock::now() < deadline && !after_repair.is_leader;) {
    int node = cluster.wait_for_one_leader(3s);
    if (node < 0) continue;
    after_repair = cluster.node(node)->propose("after-repair");
  }
  ASSERT_TRUE(after_repair.is_leader) << "no leader accepted a command after repair";
  EXPECT_GE(after_repair.index, 2);
  EXPECT_LE(after_repair.index, 3);

  ASSERT_GT(cluster.propose_until_applied("converged", /*applied_on=*/5, 15s), 0)
      << "the repaired cluster never got all five nodes to agree";
}

// A disconnected leader piles up entries that can never commit; when it
// rejoins, the history the majority chose is the one that wins.
TEST(RaftReplication, RejoinOfDisconnectedLeader) {
  RaftGroup cluster(3);
  cluster.start_all();
  ASSERT_EQ(cluster.propose_until_applied("base", /*applied_on=*/3, 5s), 1)
      << "the healthy cluster never committed a first command at index 1";

  int first_leader = cluster.wait_for_one_leader(2s);
  ASSERT_GE(first_leader, 0) << "no single leader";
  cluster.disconnect(first_leader);
  for (int entry = 1; entry <= 3; ++entry) {
    ASSERT_TRUE(cluster.node(first_leader)
                    ->propose("stale-" + std::to_string(entry))
                    .is_leader)
        << "the disconnected leader stopped accepting proposals";
  }

  // The surviving pair commits its own command at the same index range.
  int majority_index =
      cluster.propose_until_applied("second-leader", /*applied_on=*/2, 10s);
  ASSERT_GE(majority_index, 2) << "the survivors never elected a leader or committed";

  // Swap which side is reachable: the old leader rejoins, the new one drops out.
  int second_leader = cluster.wait_for_one_leader(3s);
  ASSERT_GE(second_leader, 0) << "no single leader among the survivors";
  cluster.disconnect(second_leader);
  cluster.reconnect(first_leader);
  ASSERT_GT(cluster.propose_until_applied("after-rejoin", /*applied_on=*/2, 15s), 0)
      << "the rejoined leader's stale log blocked the new pair from committing";

  // Everyone back: one agreed history, and the stale suffix is gone.
  cluster.reconnect(second_leader);
  ASSERT_GT(cluster.propose_until_applied("all-together", /*applied_on=*/3, 15s), 0)
      << "the reunited cluster never agreed";
  EXPECT_EQ(cluster.wait_applied(majority_index, /*applied_on=*/3, 5s), "second-leader")
      << "index " << majority_index << " did not converge on the majority's command";
}

// One round: five clients propose to the same leader at once, and every index
// handed out must end up holding the command it was handed out for. Returns ""
// on success, otherwise why it failed — a leader change mid-round is legal
// Raft, so the caller just retries; anything else is reported via ADD_FAILURE.
static std::string run_concurrent_proposal_round(RaftGroup& cluster, int round) {
  int leader = cluster.wait_for_one_leader(3s);
  if (leader < 0) return "no single leader to propose to";

  auto probe = cluster.node(leader)->propose("probe-" + std::to_string(round));
  if (!probe.is_leader) return "leader stepped down before the round started";
  const int term = probe.term;

  struct Proposal {
    std::string command;
    int index;
  };
  std::mutex proposals_mutex;
  std::vector<Proposal> proposals;
  std::vector<std::thread> clients;
  for (int client = 0; client < 5; ++client) {
    clients.emplace_back([&, client] {
      std::string command = "round-" + std::to_string(round) + "-client-" +
                            std::to_string(client);
      auto result = cluster.node(leader)->propose(command);
      if (!result.is_leader || result.term != term) return;   // term moved on
      std::lock_guard<std::mutex> lock(proposals_mutex);
      proposals.push_back({std::move(command), result.index});
    });
  }
  for (auto& client : clients) client.join();

  // Every proposal kept above was accepted by the same leader in the same
  // term, so a duplicate index or a divergent history is a bug, not a leader
  // change: report it as a failure instead of a reason to retry. Check this
  // before the count, so a term change cannot hide a duplicate.
  std::set<int> indexes;
  for (const auto& proposal : proposals) indexes.insert(proposal.index);
  if (indexes.size() != proposals.size()) {
    ADD_FAILURE() << "the leader handed out a duplicate index in term " << term;
    return "duplicate index";
  }
  if (proposals.size() != 5u) {
    return "only " + std::to_string(proposals.size()) +
           " of 5 proposals stayed in term " + std::to_string(term);
  }

  // Poll n_applied() directly rather than wait_applied(): a divergence is
  // permanent and must fail at once, and a timeout must read as "not applied
  // yet", not as an applied empty command.
  for (const auto& proposal : proposals) {
    auto deadline = std::chrono::steady_clock::now() + 5s;
    for (;;) {
      auto [count, applied] = cluster.n_applied(proposal.index);
      if (count < 0) {
        ADD_FAILURE() << "nodes disagree at index " << proposal.index;
        return "diverged";
      }
      if (count >= 3) {
        if (applied == proposal.command) break;
        // Three nodes agree on another command: legal only if the term moved.
        std::string reason = "index " + std::to_string(proposal.index) + " holds \"" +
                             applied + "\", expected \"" + proposal.command + "\"";
        if (cluster.node(leader)->current_term() == term) {
          ADD_FAILURE() << "the term never changed, yet " << reason;
        }
        return reason;
      }
      if (std::chrono::steady_clock::now() >= deadline) {
        return "index " + std::to_string(proposal.index) + " applied on only " +
               std::to_string(count) + " of 3 nodes within 5s";
      }
      std::this_thread::sleep_for(20ms);
    }
  }
  return "";
}

// Three attempts at the round above: a leader change mid-round is legal, but a
// cluster that never gets one clean round through is not.
TEST(RaftReplication, ConcurrentProposalsAgree) {
  RaftGroup cluster(3);
  cluster.start_all();

  std::string reason;
  for (int round = 0; round < 3; ++round) {
    if (round > 0) std::this_thread::sleep_for(2s);   // let the cluster settle
    reason = run_concurrent_proposal_round(cluster, round);
    if (reason.empty() || HasFailure()) return;
  }
  FAIL() << "3 rounds of concurrent proposals never agreed; last round: "
         << reason;
}

// Every RPC has a 15% chance of being dropped. Commands must still commit, and
// all five must converge once the loss stops.
TEST(RaftReplication, UnreliableAgreement) {
  RaftGroup cluster(5);
  cluster.net()->set_loss(0.15, 42);
  cluster.start_all();
  for (int command_number = 1; command_number <= 20; ++command_number) {
    std::string command = "unreliable-" + std::to_string(command_number);
    ASSERT_GT(cluster.propose_until_applied(command, /*applied_on=*/3, 30s), 0)
        << command << " never committed under loss";
  }
  cluster.net()->set_loss(0.0);
  ASSERT_GT(cluster.propose_until_applied("converged", /*applied_on=*/5, 30s), 0)
      << "the cluster never converged after the loss stopped";
}

// Cut every node off, then reconnect exactly `reachable`. The phases of the
// back-off test below open with one of these, so each block starts by saying
// who can talk to whom.
static void connect_only(RaftGroup& cluster, const std::vector<int>& reachable) {
  for (int node = 0; node < cluster.size(); ++node) cluster.disconnect(node);
  for (int node : reachable) cluster.reconnect(node);
}

// Append `count` entries to a leader connected to only one follower. It still
// accepts them — a leader cannot tell it is isolated — but no majority ever
// acknowledges them, so its commit index must not move. These uncommitted
// entries form a conflicting suffix that a future leader must replace.
static void propose_entries_that_cannot_commit(RaftGroup& cluster, int leader,
                                               const std::string& tag, int count) {
  const int commit_before = cluster.node(leader)->commit_index();
  for (int entry = 1; entry <= count; ++entry) {
    ASSERT_TRUE(
        cluster.node(leader)->propose(tag + "-" + std::to_string(entry)).is_leader)
        << "the isolated leader stopped accepting proposals";
  }
  std::this_thread::sleep_for(500ms);   // wait long enough to expose an incorrect commit
  EXPECT_EQ(cluster.node(leader)->commit_index(), commit_before)
      << "a minority of 2/5 committed something";
}

// Commit `tag`-1 .. `tag`-`count` through whoever the reachable majority
// elects, one after another, each agreed by `applied_on` nodes.
static void commit_numbered_commands(RaftGroup& cluster, const std::string& tag,
                                     int count, int applied_on,
                                     std::chrono::milliseconds per_command) {
  for (int entry = 1; entry <= count; ++entry) {
    std::string command = tag + "-" + std::to_string(entry);
    ASSERT_GT(cluster.propose_until_applied(command, applied_on, per_command), 0)
        << command << " never committed";
  }
}

// The next_index_ back-off test. It creates divergent logs twice: a leader
// isolated with one follower appends entries that cannot commit, while a
// majority commits *different* commands at the same indexes.
// After connectivity is restored, the current leader must move next_index_
// backward over the 20-entry uncommitted suffix before the logs match.
//
// Five nodes, named for the role they play (which node id lands in which role
// depends on who wins the first election):
//
//   A = first_leader   B = lone_follower   S = swing_node   T, U = the rest
//
//   phase   reachable     what happens
//   -----   -----------   -------------------------------------------------
//     0     A B S T U     "base" commits at index 1 — the one common prefix
//     1     A B           A appends 20 "minority-*" that can never commit
//     2         S T U     this majority commits 20 "majority-*" at those indexes
//     3           T U     their leader appends 20 more entries that cannot commit
//     4     A B S         S has the committed history; A and B have the same
//                         uncommitted suffix, which S replaces
//     5     A B S T U     everyone converges, T and U included
//
// (MockNetwork's disconnect cuts both directions, so nodes isolated in
// different phases cannot talk to each other either.)
TEST(RaftReplication, BacksUpOverIncorrectFollowerLogs) {
  RaftGroup cluster(5);
  cluster.start_all();

  int first_leader = -1, lone_follower = -1, majority_leader = -1, swing_node = -1;
  std::vector<int> majority;

  {
    SCOPED_TRACE("phase 0 — all five agree on one entry, the common prefix");
    first_leader = cluster.wait_for_one_leader(3s);
    ASSERT_GE(first_leader, 0) << "no single leader";
    ASSERT_EQ(cluster.propose_to_leader("base"), 1);
    ASSERT_EQ(cluster.wait_applied(1, /*applied_on=*/5, 5s), "base");

    std::vector<int> others = cluster.other_nodes(first_leader);
    lone_follower = others[0];                          // B, isolated with A
    majority.assign(others.begin() + 1, others.end());  // S, T, U
  }
  {
    SCOPED_TRACE("phase 1 — cut down to A+B, the old leader writes 20 dead entries");
    connect_only(cluster, {first_leader, lone_follower});
    propose_entries_that_cannot_commit(cluster, first_leader, "minority", 20);
  }
  {
    SCOPED_TRACE("phase 2 — the other three commit their own history at those indexes");
    connect_only(cluster, majority);
    ASSERT_GE(cluster.wait_for_one_leader(8s), 0) << "the majority elected nobody";
    commit_numbered_commands(cluster, "majority", 20, /*applied_on=*/3, 15s);
  }
  {
    SCOPED_TRACE("phase 3 — isolate the majority's leader with one follower");
    majority_leader = cluster.wait_for_one_leader(5s);
    ASSERT_GE(majority_leader, 0) << "the majority lost its leader";
    swing_node = (majority[0] == majority_leader) ? majority[1] : majority[0];
    cluster.disconnect(swing_node);   // leaves the majority's leader at 2 of 5
    propose_entries_that_cannot_commit(cluster, majority_leader, "second-minority", 20);
  }
  {
    SCOPED_TRACE("phase 4 — S replaces the uncommitted suffix on A and B");
    // A and B carry "minority-*"; the swing node carries the committed
    // "majority-*". The swing node's log is the only one up-to-date enough to
    // win, and it must move next_index_ backward before replacing both suffixes.
    connect_only(cluster, {first_leader, lone_follower, swing_node});
    ASSERT_GE(cluster.wait_for_one_leader(10s), 0)
        << "the three-node group never elected a leader";
    commit_numbered_commands(cluster, "healed", 20, /*applied_on=*/3, 20s);
  }
  {
    SCOPED_TRACE("phase 5 — everyone back on one history");
    connect_only(cluster, {0, 1, 2, 3, 4});
    ASSERT_GE(cluster.wait_for_one_leader(10s), 0) << "the reunited cluster has no leader";
    ASSERT_GT(cluster.propose_until_applied("final", /*applied_on=*/5, 20s), 0)
        << "all five never agreed again";
    EXPECT_EQ(cluster.wait_applied(2, /*applied_on=*/5, 5s), "majority-1")
        << "index 2 did not converge on the first majority's command";
  }
}
