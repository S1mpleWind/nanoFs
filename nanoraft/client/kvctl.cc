// spec · nanoraft_kvctl — a tiny KV client for smoke/manual testing. Sends a kv_*
// RPC to a node; on a NotLeader reply (ok=false) it retries the hinted leader (or
// the next node if the hint is unknown), so a client always finds the leader.
//
// usage:
//   kvctl --peers h0:p0,h1:p1,h2:p2 put <key> <value> [--client cid --seq n]
//   kvctl --peers h0:p0,h1:p1,h2:p2 get  <key>
//   kvctl --peers h0:p0,h1:p1,h2:p2 scan <prefix>
//
// Exit 0 on success (prints the result), 1 on failure to reach a leader.

#include "nanoraft/kv_service.h"
#include "nanoraft/kv_state_machine.h"   // decode_scan for printing
#include <rpc/client.h>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <thread>
#include <utility>
#include <vector>

using namespace nanoraft;

static std::vector<std::pair<std::string, uint16_t>> parse_peers(const std::string& s) {
  std::vector<std::pair<std::string, uint16_t>> peers;
  size_t i = 0;
  while (i < s.size()) {
    size_t comma = s.find(',', i);
    std::string tok = s.substr(i, comma == std::string::npos ? std::string::npos : comma - i);
    size_t colon = tok.rfind(':');
    peers.emplace_back(tok.substr(0, colon),
                       static_cast<uint16_t>(std::atoi(tok.substr(colon + 1).c_str())));
    if (comma == std::string::npos) break;
    i = comma + 1;
  }
  return peers;
}

int main(int argc, char** argv) {
  std::string peers_str, op, key, value, client_id = "kvctl";
  uint64_t seq = 1;
  int start = 0;   // index of the node to contact first (redirect handles the rest)
  std::vector<std::string> pos;
  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    auto next = [&]() -> std::string { return (i + 1 < argc) ? argv[++i] : std::string(); };
    if (a == "--peers") peers_str = next();
    else if (a == "--client") client_id = next();
    else if (a == "--seq") seq = std::strtoull(next().c_str(), nullptr, 10);
    else if (a == "--start") start = std::atoi(next().c_str());
    else pos.push_back(a);
  }
  if (pos.empty()) { std::fprintf(stderr, "need an op (put/get/scan)\n"); return 2; }
  op = pos[0];
  if (op == "put") {
    if (pos.size() < 3) { std::fprintf(stderr, "put needs <key> <value>\n"); return 2; }
    key = pos[1]; value = pos[2];
  } else {
    if (pos.size() < 2) { std::fprintf(stderr, "%s needs <key>\n", op.c_str()); return 2; }
    key = pos[1];
  }
  auto peers = parse_peers(peers_str);
  if (peers.empty()) { std::fprintf(stderr, "need --peers\n"); return 2; }
  int n = static_cast<int>(peers.size());

  int cur = (start >= 0 && start < n) ? start : 0;
  for (int attempt = 0; attempt < 4 * n; ++attempt) {
    KvReply rep;
    bool got = false;
    try {
      rpc::client c(peers[cur].first, peers[cur].second);
      c.set_timeout(1000);
      if (op == "put")       rep = c.call(kRpcKvPut, key, value, client_id, seq).as<KvReply>();
      else if (op == "get")  rep = c.call(kRpcKvGet, key).as<KvReply>();
      else if (op == "scan") rep = c.call(kRpcKvScan, key).as<KvReply>();
      else { std::fprintf(stderr, "unknown op '%s'\n", op.c_str()); return 2; }
      got = true;
    } catch (const std::exception&) { /* node down/timeout — try the next */ }

    if (got && rep.ok) {
      if (op == "scan") {
        for (auto& [k, v] : decode_scan(rep.result)) std::printf("%s=%s\n", k.c_str(), v.c_str());
      } else {
        std::printf("%s\n", rep.result.c_str());
      }
      return 0;
    }
    // redirect: prefer the hint, else round-robin to the next node
    cur = (got && rep.leader_hint >= 0 && rep.leader_hint < n) ? rep.leader_hint : (cur + 1) % n;
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  std::fprintf(stderr, "kvctl: could not reach a leader\n");
  return 1;
}
