// spec · Durable Raft state. Persists exactly the three pieces Fig. 2 requires to
// survive a crash — currentTerm, votedFor, log[]. Volatile state (commitIndex,
// lastApplied, role, leader cursors) is deliberately excluded: it rebuilds on
// restart. A RaftStorage must outlive its RaftNode so a restart can recover.
#pragma once
#include <vector>
#include "nanoraft/raft_protocol.h"

namespace nanoraft {

struct PersistentState {
  int current_term = 0;
  int voted_for = -1;                 // -1 = none
  std::vector<LogEntry> log;          // entries AFTER snapshot_index (Raft index snapshot_index+1 = log[0])
  int snapshot_index = 0;             // last index included in `snapshot` (0 = no snapshot)
  int snapshot_term = 0;
  std::string snapshot;               // state-machine snapshot bytes
  MSGPACK_DEFINE_ARRAY(current_term, voted_for, log, snapshot_index, snapshot_term, snapshot)
};

class RaftStorage {
 public:
  virtual ~RaftStorage() = default;
  virtual void save(const PersistentState& state) = 0;  // atomic (all-or-nothing) durable write
  virtual PersistentState load() = 0;                   // default-constructed if nothing saved
};

}  // namespace nanoraft
