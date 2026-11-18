// spec · On-disk RaftStorage. save() serializes PersistentState (msgpack) and
// replaces the file atomically (write `path.tmp`, then rename over `path`), so a
// crash mid-write never leaves a torn state file. load() returns the persisted
// state, or a default PersistentState if the file is absent or empty.
#pragma once
#include <string>
#include "nanoraft/raft_storage.h"

namespace nanoraft {

class FileStorage : public RaftStorage {
 public:
  explicit FileStorage(std::string path);
  void save(const PersistentState& state) override;
  PersistentState load() override;

 private:
  std::string path_;
};

}  // namespace nanoraft
