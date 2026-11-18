#include "raft_test_group.h"
#include <gtest/gtest.h>
#include <chrono>
#include <random>
#include <string>

using namespace nanoraft;
using namespace std::chrono_literals;

// Builds the RequestVote RPC sent by `candidate_id`. A last-log index/term of
// 0/0 represents an empty candidate log.
static RequestVoteArgs make_request_vote(int term, int candidate_id,
                                         int last_log_index,
                                         int last_log_term) {
  return RequestVoteArgs{.term = term,
                         .candidate_id = candidate_id,
                         .last_log_index = last_log_index,
                         .last_log_term = last_log_term};
}

// Fig. 2 receiver, deterministic (one unstarted node, no threads, no timing): a
// server casts at most one vote per term (§5.2) — the invariant behind "at most
// one leader per term". A retry from the same candidate is granted again (its
// first reply may have been lost), a higher term clears the vote, and a stale
// term is rejected outright.
TEST(RaftElection, GrantsAtMostOneVotePerTerm) {
  ListStateMachine state_machine;
  MockNetwork network;

  // Node 0 is the receiver of the RequestVote RPCs below. Candidates 1
  // and 2 are represented by candidate_id in each request.
  RaftNode voter(0, 3, &network, &state_machine);

  const auto candidate_1_term_1 =
      make_request_vote(/*term=*/1, /*candidate_id=*/1,
                        /*last_log_index=*/0, /*last_log_term=*/0);
  const auto candidate_2_term_1 =
      make_request_vote(/*term=*/1, /*candidate_id=*/2,
                        /*last_log_index=*/0, /*last_log_term=*/0);

  // Candidate 1 asks node 0 for its vote in term 1.
  auto first_reply = voter.handle_request_vote(candidate_1_term_1);
  EXPECT_TRUE(first_reply.vote_granted);
  EXPECT_EQ(first_reply.term, 1);

  // Candidate 2 cannot receive node 0's vote in the same term.
  auto rival_reply = voter.handle_request_vote(candidate_2_term_1);
  EXPECT_FALSE(rival_reply.vote_granted)
      << "node 0 already voted for candidate 1 in term 1";

  // Candidate 1 may retry if the first reply was lost.
  auto retry_reply = voter.handle_request_vote(candidate_1_term_1);
  EXPECT_TRUE(retry_reply.vote_granted)
      << "a retried request from the chosen candidate must be granted again";

  // A higher term frees node 0 to vote for a new candidate.
  const auto candidate_2_term_2 =
      make_request_vote(/*term=*/2, /*candidate_id=*/2,
                        /*last_log_index=*/0, /*last_log_term=*/0);
  auto new_term_reply = voter.handle_request_vote(candidate_2_term_2);
  EXPECT_TRUE(new_term_reply.vote_granted)
      << "a higher term clears votedFor";

  // Once node 0 has advanced to term 2, it rejects requests from term 1 and
  // reports its current term to the stale candidate.
  auto stale_reply = voter.handle_request_vote(candidate_1_term_1);
  EXPECT_FALSE(stale_reply.vote_granted);
  EXPECT_EQ(stale_reply.term, 2);
}

// §5.4.1 up-to-date, deterministic: the comparison is lexicographic on
// (last_log_term, last_log_index) — compare last-log terms first, and only on
// a tie does the longer log win — never the index alone. A candidate that
// cannot hold every committed entry must not win. Each case arrives in a fresh
// term, so the rejection is the log check, not a vote already cast.
// Seeds the voter's log through the §2.2 AppendEntries receiver, so get
// `RaftAppendEntries` green first.
TEST(RaftElection, RejectsCandidateWithStaleLog) {
  ListStateMachine state_machine;
  MockNetwork network;
  // Five nodes, so every RequestVote below comes from a peer whose log can
  // really exist. Node 1 is the term-2 leader that sends seed_log.
  RaftNode voter(0, 5, &network, &state_machine);

  // Give node 0 this log:
  //   index 1 -> term 1
  //   index 2 -> term 2
  //   index 3 -> term 2
  // Its last-log position is therefore (term 2, index 3).
  const AppendEntriesArgs seed_log{
      .term = 2,
      .leader_id = 1,
      .entries = {LogEntry{1, "a"}, LogEntry{2, "b"}, LogEntry{2, "c"}},
  };
  auto append_reply = voter.handle_append_entries(seed_log);
  ASSERT_TRUE(append_reply.success);

  // Node 2 still holds 9 entries an old term-1 leader replicated to it and
  // never committed; it has heard nothing since. A lower last-log term loses
  // even when the candidate has more entries.
  const auto older_term_candidate =
      make_request_vote(/*term=*/3, /*candidate_id=*/2,
                        /*last_log_index=*/9, /*last_log_term=*/1);
  auto older_term_reply = voter.handle_request_vote(older_term_candidate);
  EXPECT_FALSE(older_term_reply.vote_granted)
      << "lower last_log_term loses even with a far longer log";

  // Node 3 received a and b from the term-2 leader but missed c. With equal
  // last-log terms, the candidate with the shorter log loses.
  const auto shorter_log_candidate =
      make_request_vote(/*term=*/4, /*candidate_id=*/3,
                        /*last_log_index=*/2, /*last_log_term=*/2);
  auto shorter_log_reply = voter.handle_request_vote(shorter_log_candidate);
  EXPECT_FALSE(shorter_log_reply.vote_granted)
      << "same last_log_term, shorter log loses";

  // Node 1 — the old term-2 leader itself, whose log matches node 0's exactly —
  // campaigns again in term 5. An equal last-log position is sufficiently
  // up-to-date to receive the vote.
  const auto equally_up_to_date_candidate =
      make_request_vote(/*term=*/5, /*candidate_id=*/1,
                        /*last_log_index=*/3, /*last_log_term=*/2);
  auto up_to_date_reply =
      voter.handle_request_vote(equally_up_to_date_candidate);
  EXPECT_TRUE(up_to_date_reply.vote_granted)
      << "equal (term, index) is 'at least as up-to-date' -> grant";
}

// A healthy 3-node cluster elects exactly one leader, and every node ends up
// in that leader's term — a voter that grants a vote without adopting the
// candidate's term leaves the cluster split across terms, which no
// single-leader check catches. (That the term then STAYS put needs §2.2's
// heartbeats; `RaftReplication.HeartbeatsKeepTermsStable` is where that half
// is checked.)
TEST(RaftElection, ElectsSingleLeader) {
  RaftGroup cluster(3);
  cluster.start_all();

  int elected_leader = cluster.wait_for_one_leader(2s);

  ASSERT_GE(elected_leader, 0);
  EXPECT_EQ(cluster.leaders().size(), 1u);

  int cluster_term = cluster.wait_for_agreed_term(2s);
  EXPECT_GE(cluster_term, 1)
      << "nodes never agreed on a term (-1 = still disagreeing at timeout)";
  EXPECT_EQ(cluster_term, cluster.node(elected_leader)->current_term())
      << "the cluster settled on a term that is not the leader's";
}

// Cut the leader off; the remaining majority elects a new one. The old leader,
// rejoining at a stale term, adopts the newer term and steps down instead of
// disturbing the new leader.
TEST(RaftElection, ReelectsAfterLeaderDisconnectAndOldLeaderStepsDownOnRejoin) {
  RaftGroup cluster(3);
  cluster.start_all();

  int initial_leader = cluster.wait_for_one_leader(2s);
  ASSERT_GE(initial_leader, 0);
  int initial_term = cluster.node(initial_leader)->current_term();

  cluster.disconnect(initial_leader);
  int new_leader = cluster.wait_for_one_leader(2s);
  ASSERT_GE(new_leader, 0);
  EXPECT_NE(new_leader, initial_leader);

  // On rejoin, the old leader learns the newer term and becomes a follower.
  cluster.reconnect(initial_leader);
  std::this_thread::sleep_for(1s);

  EXPECT_FALSE(cluster.node(initial_leader)->is_leader());
  EXPECT_GE(cluster.node(initial_leader)->current_term(), initial_term);
  EXPECT_EQ(cluster.leaders().size(), 1u);
}

// Continues the test above: a majority is the only thing that elects. Take the
// leader and one follower off the network and the single node left keeps
// timing out without ever collecting two votes; hand one node back and the
// restored majority must elect again.
TEST(RaftElection, ElectsAgainOnlyOnceQuorumReturns) {
  RaftGroup cluster(3);
  cluster.start_all();

  int initial_leader = cluster.wait_for_one_leader(2s);
  ASSERT_GE(initial_leader, 0);
  int follower = cluster.some_follower(initial_leader);

  // One of three is left connected: every election it starts must fail.
  cluster.disconnect(initial_leader);
  cluster.disconnect(follower);
  std::this_thread::sleep_for(1500ms);
  EXPECT_TRUE(cluster.leaders().empty())
      << "a node declared itself leader without a majority";

  // Two of three can reach each other again — that is a majority.
  cluster.reconnect(follower);
  EXPECT_GE(cluster.wait_for_one_leader(2s), 0)
      << "the restored majority never elected a leader";

  // The last node rejoins carrying whatever term its solitary elections reached.
  // It may depose the sitting leader; the cluster must still settle on one.
  cluster.reconnect(initial_leader);
  EXPECT_GE(cluster.wait_for_one_leader(3s), 0)
      << "the rejoining node left the cluster with no single leader";
}

// Churn on a 7-node cluster: five rounds, each dropping three randomly chosen
// nodes (ids may repeat, so one to three actually go down) and demanding a
// single leader among the four that certainly remain — either the old leader
// survived, or the remaining majority elected a new one. The cluster size is
// the point: a majority rule that is accidentally right at n=3 can still be
// wrong at n=7, and the wider the cluster the more often two candidates split
// the vote and have to resolve it in a later term.
TEST(RaftElection, ManyElections) {
  constexpr int cluster_size = 7;
  std::mt19937 random(11);

  RaftGroup cluster(cluster_size);
  cluster.start_all();

  for (int round = 1; round <= 5; ++round) {
    SCOPED_TRACE("round " + std::to_string(round));
    ASSERT_GE(cluster.wait_for_one_leader(3s), 0);

    std::vector<int> downed;
    for (int i = 0; i < 3; ++i) downed.push_back(random() % cluster_size);
    for (int node : downed) cluster.disconnect(node);

    EXPECT_GE(cluster.wait_for_one_leader(3s), 0)
        << "the connected majority never settled on one leader";

    for (int node : downed) cluster.reconnect(node);
  }

  EXPECT_GE(cluster.wait_for_one_leader(3s), 0)
      << "the whole cluster is back and still has no single leader";
}

// A node that cannot reach a majority must never declare itself leader — even
// as it keeps timing out and starting elections. Isolate node 0 from the start
// (before any leader exists), so it was never a leader and can win no votes;
// nodes 1 and 2 remain a majority and do elect a leader.
TEST(RaftElection, NoLeaderWithoutMajority) {
  constexpr int isolated_node = 0;

  RaftGroup cluster(3);
  cluster.start_all();
  cluster.disconnect(isolated_node);

  // Node 0 experiences several election timeouts but cannot collect a majority.
  std::this_thread::sleep_for(1500ms);
  EXPECT_FALSE(cluster.node(isolated_node)->is_leader());

  // The connected pair {1, 2} is a majority and must elect one of its members.
  int majority_leader = cluster.wait_for_one_leader(2s);
  EXPECT_TRUE(majority_leader == 1 || majority_leader == 2);
}
