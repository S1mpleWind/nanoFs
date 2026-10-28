// PROVIDED — do not edit, do not submit. The KV command/scan wire codec
// (msgpack) plus the two halves of KvStateMachine that carry no lab content:
// scan_prefix (an ordered-map prefix walk) and snapshot/restore (pack and unpack
// the store together with the dedup table, so exactly-once Put survives a restore).
// KvStateMachine::apply, also provided, lives in src/kv_state_machine.cc and calls
// into this file.
#include "nanoraft/kv_state_machine.h"
#include <tuple>
#include <type_traits>       // libc++/C++23: pull in traits before msgpack (repo note)
#include <rpc/msgpack.hpp>

namespace nanoraft {

std::string encode(const KvCommand& c) {
  clmdep_msgpack::sbuffer sb;
  clmdep_msgpack::pack(sb, std::make_tuple(static_cast<int>(c.op), c.key, c.value,
                                           c.client_id, c.req_seq));
  return std::string(sb.data(), sb.size());
}

KvCommand decode(const std::string& bytes) {
  auto oh = clmdep_msgpack::unpack(bytes.data(), bytes.size());
  auto [op, key, value, cid, seq] =
      oh.get().as<std::tuple<int, std::string, std::string, std::string, uint64_t>>();
  return KvCommand{static_cast<KvOp>(op), key, value, cid, seq};
}

std::string encode_scan(const std::vector<std::pair<std::string, std::string>>& rows) {
  clmdep_msgpack::sbuffer sb;
  clmdep_msgpack::pack(sb, rows);
  return std::string(sb.data(), sb.size());
}

std::vector<std::pair<std::string, std::string>> decode_scan(const std::string& bytes) {
  auto oh = clmdep_msgpack::unpack(bytes.data(), bytes.size());
  return oh.get().as<std::vector<std::pair<std::string, std::string>>>();
}

// Collect all (key, value) whose key starts with `prefix`, in key order (kv_ is
// ordered, so lower_bound(prefix) + a prefix check yields sorted results).
std::vector<std::pair<std::string, std::string>>
KvStateMachine::scan_prefix(const std::string& prefix) const {
  std::vector<std::pair<std::string, std::string>> rows;
  for (auto it = kv_.lower_bound(prefix); it != kv_.end(); ++it) {
    if (it->first.compare(0, prefix.size(), prefix) != 0) break;
    rows.push_back(*it);
  }
  return rows;
}

std::string KvStateMachine::snapshot() const {
  clmdep_msgpack::sbuffer sb;
  clmdep_msgpack::pack(sb, std::make_tuple(kv_, last_seq_, last_result_));
  return std::string(sb.data(), sb.size());
}

void KvStateMachine::restore(const std::string& snap) {
  if (snap.empty()) return;
  auto oh = clmdep_msgpack::unpack(snap.data(), snap.size());
  auto [kv, seq, res] = oh.get().as<std::tuple<
      std::map<std::string, std::string>,
      std::unordered_map<std::string, uint64_t>,
      std::unordered_map<std::string, std::string>>>();
  kv_ = std::move(kv);
  last_seq_ = std::move(seq);
  last_result_ = std::move(res);
}

}  // namespace nanoraft
