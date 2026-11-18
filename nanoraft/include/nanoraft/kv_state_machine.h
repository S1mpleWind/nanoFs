// spec · A generic replicated key-value state machine over Raft. Client ops are
// KvCommands (Put/Get/Scan) encoded to opaque bytes; apply() mutates an ordered
// map and returns the command's result. Mutations are exactly-once per
// (client_id, req_seq) — a replayed seq returns the cached result instead of
// re-applying. snapshot()/restore() round-trip the whole store + dedup table.
#pragma once
#include <cstdint>
#include <map>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include "nanoraft/raft_state_machine.h"

namespace nanoraft {

enum class KvOp { Put, Get, Scan };

struct KvCommand {
  KvOp op = KvOp::Get;
  std::string key;
  std::string value;
  std::string client_id;   // "" for reads (no dedup needed)
  uint64_t req_seq = 0;
};

// KvCommand <-> opaque command bytes (msgpack). encode() output is what gets
// proposed to Raft as a log command; decode() is its inverse.
std::string encode(const KvCommand& cmd);
KvCommand   decode(const std::string& bytes);

// Scan result <-> bytes: a sorted list of (key, value) pairs.
std::string encode_scan(const std::vector<std::pair<std::string, std::string>>& rows);
std::vector<std::pair<std::string, std::string>> decode_scan(const std::string& bytes);

class KvStateMachine : public RaftStateMachine {
 public:
  // Apply one committed command. Put -> "" (deduped by client/seq); Get -> the value
  // at key (or ""); Scan -> encode_scan of all (key,value) with `key` as prefix, sorted.
  std::string apply(const std::string& command) override;
  std::string snapshot() const override;            // pack {kv_, last_seq_, last_result_}
  void restore(const std::string& snap) override;   // inverse of snapshot()

 private:
  std::vector<std::pair<std::string, std::string>> scan_prefix(const std::string& prefix) const;

  std::map<std::string, std::string> kv_;                   // ordered → sorted scans
  std::unordered_map<std::string, uint64_t> last_seq_;      // exactly-once: highest applied seq/client
  std::unordered_map<std::string, std::string> last_result_;
};

}  // namespace nanoraft
