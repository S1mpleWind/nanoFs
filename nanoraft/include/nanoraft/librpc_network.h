// spec · RpcNetwork implemented over librpc (rpclib). One rpc::client per peer,
// lazily constructed. Every send_* is time-bounded (set_timeout) and non-throwing:
// an rpc timeout / peer-down / decode error becomes std::nullopt, and the peer's
// client is dropped so the next call reconnects. Satisfies the "time-bounded,
// non-throwing" transport contract Raft relies on.
#pragma once
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include "nanoraft/rpc_network.h"

namespace rpc { class client; }

namespace nanoraft {

class LibrpcNetwork : public RpcNetwork {
 public:
  // peers is indexed by node id (peers[self_id] is this node's own address, unused
  // for sending). timeout_ms bounds every RPC; keep timeout_ms + the leader's
  // 100ms heartbeat interval under the 300ms election-timeout floor.
  LibrpcNetwork(int self_id, std::vector<std::pair<std::string, uint16_t>> peers,
                int timeout_ms);
  ~LibrpcNetwork() override;

  std::optional<RequestVoteReply>
  send_request_vote(int from, int to, const RequestVoteArgs& args) override;
  std::optional<AppendEntriesReply>
  send_append_entries(int from, int to, const AppendEntriesArgs& args) override;
  std::optional<InstallSnapshotReply>
  send_install_snapshot(int from, int to, const InstallSnapshotArgs& args) override;

 private:
  rpc::client& client(int to);   // lazily construct the peer's client (mu_ held)
  void drop_client(int to);      // erase a peer's client so the next call reconnects

  int self_id_;
  int timeout_ms_;
  std::vector<std::pair<std::string, uint16_t>> peers_;
  std::mutex mu_;
  std::unordered_map<int, std::unique_ptr<rpc::client>> clients_;
};

}  // namespace nanoraft
