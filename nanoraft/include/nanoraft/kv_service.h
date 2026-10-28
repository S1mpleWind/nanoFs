// spec · The client↔cluster wire contract for the KV service. Shared by nanoraftd
// (binds) and any client (calls). A KvReply with ok=false means "I am not the
// leader": retry `leader_hint` (or, if it is -1, another node round-robin).
#pragma once
#include <cstdint>
#include <string>
#include <type_traits>       // libc++/C++23: traits before msgpack (repo note)
#include <rpc/msgpack.hpp>

namespace nanoraft {

struct KvReply {
  bool ok = false;
  int leader_hint = -1;
  std::string result;        // Get/Scan bytes; empty for Put
  MSGPACK_DEFINE_ARRAY(ok, leader_hint, result)
};

// RPC method names — one source of truth for binds and calls.
inline constexpr const char* kRpcRequestVote     = "raft_request_vote";
inline constexpr const char* kRpcAppendEntries   = "raft_append_entries";
inline constexpr const char* kRpcInstallSnapshot = "raft_install_snapshot";
inline constexpr const char* kRpcKvPut  = "kv_put";
inline constexpr const char* kRpcKvGet  = "kv_get";
inline constexpr const char* kRpcKvScan = "kv_scan";

}  // namespace nanoraft
