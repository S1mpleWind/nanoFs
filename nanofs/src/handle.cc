#include "nanofs/handle.h"

namespace nanofs {

Result<inode_id> resolve_handle(InodeManager& inodes, FileHandle h) {
  /* TODO Lab1B: Your code here */
  //0. get data
  inode_id id = h.id;
  uint32_t generation = h.generation;

  //1. Inode number check
  if (id == 0) return std::unexpected(Error::Stale);

  //2. Inode check (get also rejects out-of-range inode numbers)
  auto where = inodes.get(id);//get block id
  if (!where) {
    if (where.error() == Error::NotFound) return std::unexpected(Error::Stale);
    return std::unexpected(where.error());
  }

  //3. generation num check
  auto current_generation = inodes.generation(id);
  if (!current_generation) {
    if (current_generation.error() == Error::NotFound)
      return std::unexpected(Error::Stale);
    return std::unexpected(current_generation.error());
  }
  if (*current_generation != generation) return std::unexpected(Error::Stale);
  return id;
}

}  // namespace nanofs
