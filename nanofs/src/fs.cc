// FS layer implementation (spec: include/nanofs/fs.h)
#include "nanofs/fs.h"
#include <cstring>
#include <algorithm>
#include <limits>

namespace nanofs {

Result<Superblock> FsLayer::super() {
  std::vector<std::byte> buf(dev_.block_size());
  if (auto r = dev_.read_block(0, buf); !r) return std::unexpected(r.error());
  Superblock sb{};
  std::memcpy(&sb, buf.data(), sizeof(sb));
  return sb;
}

// Read one block as an array of block_id (an indirect block).
static Result<std::vector<block_id>> read_ptrs(BlockDevice& dev, block_id blk) {
  std::vector<std::byte> buf(dev.block_size());
  if (auto r = dev.read_block(blk, buf); !r) return std::unexpected(r.error());
  std::vector<block_id> ptrs(dev.block_size() / sizeof(block_id));
  std::memcpy(ptrs.data(), buf.data(), ptrs.size() * sizeof(block_id));
  return ptrs;
}
static Result<void> write_ptrs(BlockDevice& dev, block_id blk, const std::vector<block_id>& ptrs) {
  std::vector<std::byte> buf(dev.block_size(), std::byte{0});
  std::memcpy(buf.data(), ptrs.data(), std::min(buf.size(), ptrs.size() * sizeof(block_id)));
  return dev.write_block(blk, buf);
}

Result<block_id> FsLayer::logical_to_physical_block(inode& node, uint32_t logical_idx,
                                                    bool allocate,
                                                    BlockAllocator& balloc,
                                                    uint32_t bs) {
  /* TODO Lab1A: Your code here */
  //*1. calculate position
  //uint32_t block_id_size; //size of block_id is 4 bytes
  // 一个间接块里可以保存多少个 block_id（物理块号）。
  const uint64_t num_ptr_per_block = bs / sizeof(block_id);
  const uint64_t single_start = NDIRECT;
  const uint64_t double_start = single_start + num_ptr_per_block;
  const uint64_t capacity = double_start + num_ptr_per_block * num_ptr_per_block;

  //*0. check boundary
  // logical_idx 是 uint32_t，不会小于 0；这里只需检查上限。
  if (num_ptr_per_block == 0 || logical_idx >= capacity)
    return std::unexpected(Error::Invalid);

  //*0.5 prepare an allocate function for reuse
  // allocate=false 遇到空指针时返回 NULL_BLOCK；allocate=true 才创建缺少的块。
  // 新块先清零：间接表从 NULL_BLOCK 开始，数据块的未写部分读为 0。
  auto allocate_zeroed_block = [&]() -> Result<block_id> {
    auto id = balloc.allocate();
    if (!id) return std::unexpected(id.error());
    if (auto r = dev_.zero_block(*id); !r) {
      (void)balloc.free(*id);
      return std::unexpected(r.error());
    }
    return *id;
  };

  //*2. judge which tier level(1,2,3)
  block_id physical_idx = NULL_BLOCK;
  if (logical_idx < NDIRECT) {
    //* Tier 1: inode 直接保存数据块的物理块号。
    physical_idx = node.blocks[logical_idx];
    if (physical_idx == NULL_BLOCK) {
      if (!allocate) return NULL_BLOCK;  // 未分配的逻辑块是 hole，不是错误。
      auto id = allocate_zeroed_block();
      if (!id) return std::unexpected(id.error());
      physical_idx = *id;
      node.blocks[logical_idx] = physical_idx;
    }
    return physical_idx;
  }

  if (logical_idx < double_start) {
    //* Tier 2: inode 指向一级间接表，表项再指向数据块。
    block_id single_table_block = node.blocks[NDIRECT];

    // check single_table
    if (single_table_block == NULL_BLOCK) {
      if (!allocate) return NULL_BLOCK;
      auto id = allocate_zeroed_block();
      if (!id) return std::unexpected(id.error());
      single_table_block = *id;
      node.blocks[NDIRECT] = single_table_block;
    }
    
    //calculate inner offset
    const uint32_t single_offset =
        static_cast<uint32_t>(logical_idx - single_start);

    //fetch id from physical block, using private Block Device         
    auto single_ptrs = read_ptrs(dev_, single_table_block);
    if (!single_ptrs) return std::unexpected(single_ptrs.error());
    physical_idx = (*single_ptrs)[single_offset];
    
    // check direct node if is null
    if (physical_idx == NULL_BLOCK) {
      // disable allocate
      if (!allocate) return NULL_BLOCK;

      // enable allocate
      auto id = allocate_zeroed_block();
      if (!id) return std::unexpected(id.error());
      physical_idx = *id;
      (*single_ptrs)[single_offset] = physical_idx; //write the newly allocated block
      if (auto r = write_ptrs(dev_, single_table_block, *single_ptrs); !r) {
        (void)balloc.free(physical_idx);
        return std::unexpected(r.error());
      }
    }
    return physical_idx;
  }

  // handle double layer
  // *Tier 3: inode -> double-indirect table -> single-indirect table -> data block.
  block_id double_table_block = node.blocks[NDIRECT + 1];
  if (double_table_block == NULL_BLOCK) {
    if (!allocate) return NULL_BLOCK;
    auto id = allocate_zeroed_block();
    if (!id) return std::unexpected(id.error());
    double_table_block = *id;
    node.blocks[NDIRECT + 1] = double_table_block;
  }

  const uint64_t double_relative = logical_idx - double_start;
  const uint32_t double_offset =
      static_cast<uint32_t>(double_relative / num_ptr_per_block);
  const uint32_t offset =
      static_cast<uint32_t>(double_relative % num_ptr_per_block);

  auto double_ptrs = read_ptrs(dev_, double_table_block);
  if (!double_ptrs) return std::unexpected(double_ptrs.error());
  block_id single_table_block = (*double_ptrs)[double_offset];
  if (single_table_block == NULL_BLOCK) {
    if (!allocate) return NULL_BLOCK;
    auto id = allocate_zeroed_block();
    if (!id) return std::unexpected(id.error());
    single_table_block = *id;
    (*double_ptrs)[double_offset] = single_table_block;
    if (auto r = write_ptrs(dev_, double_table_block, *double_ptrs); !r) {
      (void)balloc.free(single_table_block);
      return std::unexpected(r.error());
    }
  }

  //*handle single layer
  // 读二级间接结构里的下一层表，再用 offset 找到实际数据块。
  auto single_ptrs = read_ptrs(dev_, single_table_block);
  if (!single_ptrs) return std::unexpected(single_ptrs.error());
  physical_idx = (*single_ptrs)[offset];
  if (physical_idx == NULL_BLOCK) {
    if (!allocate) return NULL_BLOCK;
    auto id = allocate_zeroed_block();
    if (!id) return std::unexpected(id.error());
    physical_idx = *id;
    (*single_ptrs)[offset] = physical_idx;
    if (auto r = write_ptrs(dev_, single_table_block, *single_ptrs); !r) {
      (void)balloc.free(physical_idx);
      return std::unexpected(r.error());
    }
  }
  return physical_idx;

}

Result<std::string> FsLayer::read_bytes(inode& node, uint32_t off,
                                        uint32_t len) {
  //*0. pre check, make sure read after EOF and at most read to EOF
  if (off >= node.size || len == 0) return std::string{};
  const uint32_t count = std::min<uint32_t>(len, node.size - off);
  const uint32_t bs = static_cast<uint32_t>(dev_.block_size());
  
  //*0.5 prepare allocator
  auto sb = super();
  if (!sb) return std::unexpected(sb.error());
  BlockAllocator balloc(dev_, sb->block_bitmap_start,
                        sb->block_bitmap_blocks, sb->data_start,
                        sb->num_blocks);

  std::string result(count, '\0');
  std::vector<std::byte> block(bs);

  //*1.loop to read bytes
  uint32_t done = 0;
  while (done < count) {
    const uint32_t position = off + done;
    const uint32_t logical_idx = position / bs;
    const uint32_t block_offset = position % bs;
    const uint32_t chunk = std::min(bs - block_offset, count - done);
    auto physical = logical_to_physical_block(node, logical_idx, false,
                                               balloc, bs);
    if (!physical) return std::unexpected(physical.error());
    if (*physical != NULL_BLOCK) {
      if (auto r = dev_.read_block(*physical, block); !r)
        return std::unexpected(r.error());
      std::memcpy(result.data() + done, block.data() + block_offset, chunk);
    }
    done += chunk;
  }
  return result;
}

Result<uint32_t> FsLayer::write_bytes(inode_id id, inode& node, uint32_t off,
                                      const std::string& data) {
  //*0. pre check
  if (data.empty()) return 0u;

  const uint32_t bs = static_cast<uint32_t>(dev_.block_size());
  //*0.5 prepare allocator
  auto sb = super();
  if (!sb) return std::unexpected(sb.error());
  BlockAllocator balloc(dev_, sb->block_bitmap_start,
                        sb->block_bitmap_blocks, sb->data_start,
                        sb->num_blocks);

  std::vector<std::byte> block(bs);
  uint32_t done = 0;
  //*1. write the data block by block
  while (done < data.size()) {
    const uint32_t position = off + done;
    const uint32_t logical_idx = position / bs;
    const uint32_t block_offset = position % bs;
    const uint32_t chunk = std::min(bs - block_offset,
                                    static_cast<uint32_t>(data.size() - done));

    //*1.5 map the logical block, allocating missing data and indirect blocks
    auto physical = logical_to_physical_block(node, logical_idx, true,
                                               balloc, bs);
    if (!physical) return std::unexpected(physical.error());

    //*2. preserve bytes outside the write range for a partial-block write
    if (block_offset != 0 || chunk != bs) {
      if (auto r = dev_.read_block(*physical, block); !r)
        return std::unexpected(r.error());
    }
    std::memcpy(block.data() + block_offset, data.data() + done, chunk);
    if (auto r = dev_.write_block(*physical, block, BlockKind::Data); !r)
      return std::unexpected(r.error());
    done += chunk;
  }

  //*3. grow the inode and persist its updated block pointers
  const uint32_t end = off + static_cast<uint32_t>(data.size());
  if (end > node.size) node.size = end;
  if (auto r = inodes_.write_inode(id, node); !r)
    return std::unexpected(r.error());
  return static_cast<uint32_t>(data.size());
}

Result<Attr> FsLayer::getattr(inode_id id) {
  /* TODO Lab1A: Your code here */
  return std::unexpected(Error::IoError);
}

Result<std::string> FsLayer::read_file(inode_id id, uint32_t off, uint32_t len) {
  if (off + static_cast<uint64_t>(len) > std::numeric_limits<uint32_t>::max())
    return std::unexpected(Error::Invalid);
  auto node = inodes_.read_inode(id);
  if (!node) return std::unexpected(node.error());
  if (node->type == INODE_DIR) return std::unexpected(Error::IsDir);
  return read_bytes(*node, off, len);
}

Result<uint32_t> FsLayer::write_file(inode_id id, uint32_t off,
                                     const std::string& data) {
  if (data.size() > std::numeric_limits<uint32_t>::max() ||
      off + static_cast<uint64_t>(data.size()) >
          std::numeric_limits<uint32_t>::max())
    return std::unexpected(Error::Invalid);
  auto node = inodes_.read_inode(id);
  if (!node) return std::unexpected(node.error());
  if (node->type == INODE_DIR) return std::unexpected(Error::IsDir);
  return write_bytes(id, *node, off, data);
}

// The directory layout (fs.h) is deliberately simple so the three helpers stay short.
// The cost: short names waste the rest of the slot, lookup scans linearly past holes,
// and the file is never compacted. You may go further (variable-length records,
// compaction on delete, a hash index); the tests do not require it.
Result<std::vector<DirEntry>> FsLayer::read_dir_entries(inode_id dir) {
  /* TODO Lab1A: Your code here */
  return std::unexpected(Error::IoError);
}

Result<inode_id> FsLayer::lookup(inode_id parent, const std::string& name) {
  /* TODO Lab1A: Your code here */
  return std::unexpected(Error::IoError);
}

// Provided: public entry for read_dir_entries.
Result<std::vector<DirEntry>> FsLayer::readdir(inode_id dir) {
  return read_dir_entries(dir);
}

Result<void> FsLayer::add_dir_entry(inode_id dir, const std::string& name,
                                    inode_id target) {
  /* TODO Lab1A: Your code here */
  return std::unexpected(Error::IoError);
}

Result<void> FsLayer::remove_dir_entry(inode_id dir, const std::string& name) {
  /* TODO Lab1A: Your code here */
  return std::unexpected(Error::IoError);
}

Result<inode_id> FsLayer::make_node(inode_id parent, const std::string& name,
                                    uint32_t type) {
  /* TODO Lab1A: Your code here */
  return std::unexpected(Error::IoError);
}

// Provided: create/mkdir differ only in the inode type; the work is in make_node.
Result<inode_id> FsLayer::create(inode_id parent, const std::string& name) {
  return make_node(parent, name, INODE_FILE);
}

Result<inode_id> FsLayer::mkdir(inode_id parent, const std::string& name) {
  return make_node(parent, name, INODE_DIR);
}

Result<void> FsLayer::free_file_blocks(inode& node, BlockAllocator& balloc,
                                       uint32_t bs) {
  /* TODO Lab1A: Your code here */
  return std::unexpected(Error::IoError);
}

Result<void> FsLayer::unlink(inode_id parent, const std::string& name) {
  /* TODO Lab1A: Your code here */
  return std::unexpected(Error::IoError);
}

Result<void> FsLayer::rmdir(inode_id parent, const std::string& name) {
  /* TODO Lab1A: Your code here */
  return std::unexpected(Error::IoError);
}

Result<Attr> FsLayer::setattr(inode_id id, uint32_t new_size) {
  /* TODO Lab1A: Your code here */
  return std::unexpected(Error::IoError);
}
}  // namespace nanofs
