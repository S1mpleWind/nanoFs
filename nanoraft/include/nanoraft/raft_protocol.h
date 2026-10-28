// spec · Raft RPC wire structs (msgpack-serializable via rpclib).
// These are the exact payloads of the three Raft RPCs (Raft paper Fig. 2).
// Indices are 1-based; index 0 means "empty log / before the first entry".
#pragma once
#include <cstdint>
#include <string>
#include <type_traits>
#include <vector>
#include <rpc/msgpack.hpp>

namespace nanoraft {

// One replicated log entry: the term it was created in + the opaque state-machine
// command (serialized by the application; Raft never inspects it).
struct LogEntry {
  int term = 0;
  std::string command;
  MSGPACK_DEFINE_ARRAY(term, command)
};

struct RequestVoteArgs {
  int term = 0;
  int candidate_id = 0;
  int last_log_index = 0;   // index of candidate's last log entry (0 if empty)
  int last_log_term = 0;    // term of that entry (0 if empty)
  MSGPACK_DEFINE_ARRAY(term, candidate_id, last_log_index, last_log_term)
};

struct RequestVoteReply {
  int term = 0;             // receiver's currentTerm, for the candidate to update itself
  bool vote_granted = false;
  MSGPACK_DEFINE_ARRAY(term, vote_granted)
};

struct AppendEntriesArgs {
  int term = 0;
  int leader_id = 0;
  int prev_log_index = 0;
  int prev_log_term = 0;
  std::vector<LogEntry> entries;   // empty = heartbeat
  int leader_commit = 0;
  MSGPACK_DEFINE_ARRAY(term, leader_id, prev_log_index, prev_log_term, entries, leader_commit)
};

struct AppendEntriesReply {
  int term = 0;             // receiver's currentTerm, for the leader to update itself
  bool success = false;     // did the log-consistency check at prev_log_index pass
  // Optional, ungraded (§5.3's last paragraph, the 6.824 XTerm/XIndex hint): on a
  // rejection a follower may say where the conflict starts, so the leader can skip a
  // whole term per round trip. 0/0 = no hint; decrementing next_index_ by one is
  // always correct, and every test passes with these two fields left untouched.
  int conflict_term = 0;
  int conflict_index = 0;
  MSGPACK_DEFINE_ARRAY(term, success, conflict_term, conflict_index)
};

struct InstallSnapshotArgs {
  int term = 0;
  int leader_id = 0;
  int last_included_index = 0;   // snapshot replaces log[1..last_included_index]
  int last_included_term = 0;
  std::string data;              // opaque state-machine snapshot bytes
  MSGPACK_DEFINE_ARRAY(term, leader_id, last_included_index, last_included_term, data)
};

struct InstallSnapshotReply {
  int term = 0;
  MSGPACK_DEFINE_ARRAY(term)
};

}  // namespace nanoraft
