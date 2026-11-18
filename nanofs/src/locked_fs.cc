#include "nanofs/locked_fs.h"

#include <shared_mutex>

namespace nanofs {

Result<inode_id> LockedFs::lookup(inode_id parent, const std::string& name) {
  if (!lockable(parent))
    return FsLayer::lookup(parent, name); //*用FsLayer里的实现去处理无效的，并返回相应的错误

  auto guard = locks_.lock_shared(parent);
  return FsLayer::lookup(parent, name);
}

//* ==== read only, no lock acquired ====
Result<Attr> LockedFs::getattr(inode_id id) {
  if (!lockable(id)) return FsLayer::getattr(id);
  auto guard = locks_.lock_shared(id);
  return FsLayer::getattr(id);
}

Result<std::vector<DirEntry>> LockedFs::readdir(inode_id dir) {
  if (!lockable(dir)) return FsLayer::readdir(dir);
  auto guard = locks_.lock_shared(dir);
  return FsLayer::readdir(dir);
}

Result<std::string> LockedFs::read_file(inode_id id, uint32_t off,
                                        uint32_t len) {
  if (!lockable(id)) return FsLayer::read_file(id, off, len);
  auto guard = locks_.lock_shared(id);
  return FsLayer::read_file(id, off, len);
}
//* ==== read only ====



Result<uint32_t> LockedFs::write_file(inode_id id, uint32_t off,
                                      const std::string& data) {
  if (!lockable(id)) return TxFs::write_file(id, off, data);

  //*拿 独占锁
  auto guard = locks_.lock_exclusive(id);
  //*建立分配器锁作用域
  AllocatorLock::Scope scope(alloc_lock_);
  return TxFs::write_file(id, off, data);
}

Result<Attr> LockedFs::setattr(inode_id id, uint32_t new_size) {
  if (!lockable(id)) return TxFs::setattr(id, new_size);
  auto guard = locks_.lock_exclusive(id);
  AllocatorLock::Scope scope(alloc_lock_);
  return TxFs::setattr(id, new_size);
}

Result<inode_id> LockedFs::make_node(inode_id parent, const std::string& name,
                                     uint32_t type) {
  if (!lockable(parent)) return TxFs::make_node(parent, name, type);
  auto guard = locks_.lock_exclusive(parent);
  AllocatorLock::Scope scope(alloc_lock_);
  return TxFs::make_node(parent, name, type);
}

Result<void> LockedFs::unlink(inode_id parent, const std::string& name) {
  if (!lockable(parent)) return TxFs::unlink(parent, name);
  
  //独占父目录，因为他要改父目录
  auto parent_guard = locks_.lock_exclusive(parent);
  
  std::unique_lock<std::shared_mutex> child_guard;
  
  //查到子inode 也上锁
  if (auto child = FsLayer::lookup(parent, name);
      child && lockable(*child)) {
    child_guard = locks_.lock_exclusive(*child);
  }

  AllocatorLock::Scope scope(alloc_lock_);
  return TxFs::unlink(parent, name);
}

Result<void> LockedFs::rmdir(inode_id parent, const std::string& name) {
  if (!lockable(parent)) return TxFs::rmdir(parent, name);

  auto parent_guard = locks_.lock_exclusive(parent);
  std::unique_lock<std::shared_mutex> child_guard;
  if (auto child = FsLayer::lookup(parent, name);
      child && lockable(*child)) {
    child_guard = locks_.lock_exclusive(*child);
  }

  AllocatorLock::Scope scope(alloc_lock_);
  return TxFs::rmdir(parent, name);
}

}  // namespace nanofs
