// spec · The network interface. RaftNode never talks to a transport directly; it
// sends the Raft RPCs through this interface. Production wires it to librpc;
// tests wire it to MockNetwork, which can cut a node off from every peer
// WITHOUT stopping that node's timer threads.
#pragma once
#include <optional>
#include "nanoraft/raft_protocol.h"

namespace nanoraft {

class RpcNetwork {
 public:
  virtual ~RpcNetwork() = default;

  // Deliver a RequestVote from node `from` to node `to`, return `to`'s reply.
  // std::nullopt means the message (or its reply) did not get through.
  virtual std::optional<RequestVoteReply>
  send_request_vote(int from, int to, const RequestVoteArgs& args) = 0;

  virtual std::optional<AppendEntriesReply>
  send_append_entries(int from, int to, const AppendEntriesArgs& args) = 0;

  virtual std::optional<InstallSnapshotReply>
  send_install_snapshot(int from, int to, const InstallSnapshotArgs& args) = 0;
};

}  // namespace nanoraft
