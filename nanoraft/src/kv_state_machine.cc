// PROVIDED — do not edit, do not submit. The replicated app your RaftNode drives
// (see docs/lab3/lab3-tasks.md): read it, it is what `RaftKv.*` runs your Raft against. The
// codec it calls (decode / encode_scan), the scan_prefix walk and snapshot/restore
// are next door in src/kv_codec.cc, also provided.
#include "nanoraft/kv_state_machine.h"

namespace nanoraft {

// Apply one committed KV command. decode() it, then:
//   - Get  → return the value at key (or "");
//   - Scan → return encode_scan(scan_prefix(key)) (sorted rows under the prefix);
//   - Put  → exactly-once dedup on (client_id, req_seq): if req_seq was already
//            applied for this client, return the cached result WITHOUT re-applying;
//            otherwise set kv_[key]=value, record last_seq_/last_result_, return "".
std::string KvStateMachine::apply(const std::string& command) {
  KvCommand c = decode(command);
  if (c.op == KvOp::Get) {
    auto it = kv_.find(c.key);
    return it == kv_.end() ? "" : it->second;
  }
  if (c.op == KvOp::Scan) return encode_scan(scan_prefix(c.key));

  // Put: exactly-once dedup on (client_id, req_seq).
  auto seen = last_seq_.find(c.client_id);
  if (!c.client_id.empty() && seen != last_seq_.end() && c.req_seq <= seen->second)
    return last_result_[c.client_id];
  kv_[c.key] = c.value;
  if (!c.client_id.empty()) {
    last_seq_[c.client_id] = c.req_seq;
    last_result_[c.client_id] = "";
  }
  return "";
}

}  // namespace nanoraft
