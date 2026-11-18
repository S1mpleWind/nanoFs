#include "nanoraft/result.h"
#include "nanoraft/raft_protocol.h"
#include <gtest/gtest.h>
#include <rpc/msgpack.hpp>

using nanoraft::Result;
using nanoraft::RaftError;

TEST(RaftSmoke, ResultCarriesValueOrError) {
  Result<int> ok = 42;
  ASSERT_TRUE(ok.has_value());
  EXPECT_EQ(*ok, 42);

  Result<int> bad = std::unexpected(RaftError::NotLeader);
  ASSERT_FALSE(bad.has_value());
  EXPECT_EQ(bad.error(), RaftError::NotLeader);
}

TEST(RaftSmoke, RequestVoteArgsRoundTrips) {
  nanoraft::RequestVoteArgs a{.term = 3, .candidate_id = 1,
                              .last_log_index = 7, .last_log_term = 2};
  clmdep_msgpack::sbuffer buf;
  clmdep_msgpack::pack(buf, a);
  auto oh = clmdep_msgpack::unpack(buf.data(), buf.size());
  auto b = oh.get().as<nanoraft::RequestVoteArgs>();
  EXPECT_EQ(b.term, 3);
  EXPECT_EQ(b.candidate_id, 1);
  EXPECT_EQ(b.last_log_index, 7);
  EXPECT_EQ(b.last_log_term, 2);
}
