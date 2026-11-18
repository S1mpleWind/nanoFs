#include "nanoraft/librpc_network.h"
#include "nanoraft/kv_service.h"   // RPC method names
#include <exception>
#include <rpc/client.h>

namespace nanoraft {

LibrpcNetwork::LibrpcNetwork(int self_id,
                             std::vector<std::pair<std::string, uint16_t>> peers,
                             int timeout_ms)
    : self_id_(self_id), timeout_ms_(timeout_ms), peers_(std::move(peers)) {}

LibrpcNetwork::~LibrpcNetwork() = default;

rpc::client& LibrpcNetwork::client(int to) {
  std::lock_guard<std::mutex> g(mu_);
  auto it = clients_.find(to);
  if (it == clients_.end()) {
    const auto& [host, port] = peers_.at(to);
    auto c = std::make_unique<rpc::client>(host, port);
    c->set_timeout(timeout_ms_);
    it = clients_.emplace(to, std::move(c)).first;
  }
  return *it->second;   // heap object is stable even if the map rehashes later
}

void LibrpcNetwork::drop_client(int to) {
  std::lock_guard<std::mutex> g(mu_);
  clients_.erase(to);
}

std::optional<RequestVoteReply>
LibrpcNetwork::send_request_vote(int /*from*/, int to, const RequestVoteArgs& args) {
  try {
    return client(to).call(kRpcRequestVote, args).as<RequestVoteReply>();
  } catch (const std::exception&) { drop_client(to); return std::nullopt; }
}

std::optional<AppendEntriesReply>
LibrpcNetwork::send_append_entries(int /*from*/, int to, const AppendEntriesArgs& args) {
  try {
    return client(to).call(kRpcAppendEntries, args).as<AppendEntriesReply>();
  } catch (const std::exception&) { drop_client(to); return std::nullopt; }
}

std::optional<InstallSnapshotReply>
LibrpcNetwork::send_install_snapshot(int /*from*/, int to, const InstallSnapshotArgs& args) {
  try {
    return client(to).call(kRpcInstallSnapshot, args).as<InstallSnapshotReply>();
  } catch (const std::exception&) { drop_client(to); return std::nullopt; }
}

}  // namespace nanoraft
