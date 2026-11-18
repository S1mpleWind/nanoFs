// spec · The replicated application. Raft calls apply() on each committed
// command in log order; snapshot()/restore() support log compaction (P1c).
// Raft treats `command` as opaque bytes.
#pragma once
#include <string>

namespace nanoraft {

class RaftStateMachine {
 public:
  virtual ~RaftStateMachine() = default;
  virtual std::string apply(const std::string& command) = 0;  // apply a committed entry (in order); returns the command's result ("" if none)
  virtual std::string snapshot() const = 0;            // serialize full state
  virtual void restore(const std::string& snap) = 0;   // load from a snapshot
};

}  // namespace nanoraft
