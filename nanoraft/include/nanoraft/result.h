// spec · nanoraft shared error type.
// Result<T> is a value-or-error return (std::expected). Fallible nanoraft ops
// return it instead of throwing, so every error path is explicit and leak-free.
#pragma once
#include <expected>

namespace nanoraft {

enum class RaftError {
  NotLeader,     // this node is not the leader; caller should redirect
  Timeout,       // an operation did not complete in time
  Stopped,       // the node is shutting down
  Corrupt,       // persisted/serialized data failed a validity check
  OutOfRange,    // an index/id outside the valid range
};

template <class T>
using Result = std::expected<T, RaftError>;

}  // namespace nanoraft
