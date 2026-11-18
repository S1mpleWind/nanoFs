// spec · nanoraftd — a fault-tolerant KV service node. One librpc server binds two
// planes on `--port`: node↔node Raft RPCs (raft_* → RaftNode::handle_*) and
// client↔cluster KV RPCs (kv_* → propose + await commit). A non-leader answers
// kv_* with ok=false + a leader hint so the client can redirect. Durable state
// lives in a FileStorage; peers are given as id-ordered host:port pairs.
//
// usage: nanoraftd --id N --peers h0:p0,h1:p1,h2:p2 --port P --storage path
//                  [--timeout-ms 100]

#include "nanoraft/raft.h"
#include "nanoraft/kv_state_machine.h"
#include "nanoraft/kv_service.h"
#include "nanoraft/librpc_network.h"
#include "nanoraft/file_storage.h"
#include <rpc/server.h>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <future>
#include <string>
#include <utility>
#include <vector>

using namespace nanoraft;
using namespace std::chrono_literals;

// Parse "h0:p0,h1:p1,..." into id-ordered (host, port) pairs.
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

static void usage(const char* prog) {
  std::fprintf(stderr,
               "usage: %s --id N --peers h0:p0,h1:p1,h2:p2 --port P --storage path "
               "[--timeout-ms 100]\n",
               prog);
}

int main(int argc, char** argv) {
  // rpc timeout + heartbeat interval (100ms) must stay under the 300ms election
  // floor, or one blackholed peer stretches heartbeat rounds into election range.
  int id = -1, port = 0, timeout_ms = 100;
  std::string peers_str, storage_path;
  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    auto next = [&]() -> std::string { return (i + 1 < argc) ? argv[++i] : std::string(); };
    if (a == "--id") id = std::atoi(next().c_str());
    else if (a == "--peers") peers_str = next();
    else if (a == "--port") port = std::atoi(next().c_str());
    else if (a == "--storage") storage_path = next();
    else if (a == "--timeout-ms") timeout_ms = std::atoi(next().c_str());
    else { usage(argv[0]); return 2; }
  }
  auto peers = parse_peers(peers_str);
  if (id < 0 || port == 0 || storage_path.empty() ||
      id >= static_cast<int>(peers.size())) { usage(argv[0]); return 2; }
  int n = static_cast<int>(peers.size());

  FileStorage storage(storage_path);
  KvStateMachine kv;
  LibrpcNetwork net(id, peers, timeout_ms);
  RaftNode node(id, n, &net, &kv, &storage);
  node.start();

  // Submit a KV command, await its commit, and shape the reply. On a non-leader (or
  // if leadership is lost before commit), return ok=false + a leader hint to redirect.
  auto kv_op = [&](KvOp op, std::string k, std::string v, std::string cid,
                   uint64_t seq) -> KvReply {
    auto r = node.propose(encode({op, std::move(k), std::move(v), std::move(cid), seq}));
    if (!r.is_leader) return KvReply{false, node.leader_hint(), ""};
    auto res = node.await_applied(r.index, r.term, 1000ms);
    if (!res) return KvReply{false, node.leader_hint(), ""};
    return KvReply{true, id, *res};
  };

  rpc::server srv(static_cast<uint16_t>(port));
  srv.bind(kRpcRequestVote,     [&](RequestVoteArgs a)     { return node.handle_request_vote(a); });
  srv.bind(kRpcAppendEntries,   [&](AppendEntriesArgs a)   { return node.handle_append_entries(a); });
  srv.bind(kRpcInstallSnapshot, [&](InstallSnapshotArgs a) { return node.handle_install_snapshot(a); });
  srv.bind(kRpcKvPut, [&](std::string k, std::string v, std::string cid, uint64_t seq) {
    return kv_op(KvOp::Put, std::move(k), std::move(v), std::move(cid), seq);
  });
  srv.bind(kRpcKvGet,  [&](std::string k)      { return kv_op(KvOp::Get,  std::move(k), "", "", 0); });
  srv.bind(kRpcKvScan, [&](std::string prefix) { return kv_op(KvOp::Scan, std::move(prefix), "", "", 0); });

  std::fprintf(stderr, "nanoraftd id=%d listening on :%d (n=%d)\n", id, port, n);
  srv.async_run(4);            // workers so a blocked await never stalls other RPCs
  std::promise<void>().get_future().wait();   // park until the process is killed
  return 0;
}
