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

// TODO Lab2A: classify writes here as BlockKind::Data for regular-file
// contents and BlockKind::Meta for directory contents.
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
  /*
    struct Attr { uint32_t inode_id; bool is_dir; uint32_t size; };
   */
  //* 0.get the inode
  auto node = inodes_.read_inode(id);
  if (!node) return std::unexpected(node.error());

  //* 1.get attr
  Attr attr{
      .inode_id = id,
      .is_dir = node->type == INODE_DIR,
      .size = node->size,
  };

  return attr;
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
  //* 0. read and validate the directory inode
  auto node = inodes_.read_inode(dir);
  if (!node) return std::unexpected(node.error());
  if (node->type != INODE_DIR)
    return std::unexpected(Error::NotDir);

  //* 1. read the directory file; unused slots are skipped
  //*    and convert them to 
  auto bytes = read_bytes(*node, 0, node->size);
  if (!bytes) return std::unexpected(bytes.error());

  std::vector<DirEntry> entries;//store in vec
  for (size_t off = 0; off + sizeof(DirEntry) <= bytes->size();
       off += sizeof(DirEntry)) {
    DirEntry entry{};
    std::memcpy(&entry, bytes->data() + off, sizeof(entry));
    if (entry.inode_id != 0) entries.push_back(entry);
  }
  return entries;
}

Result<inode_id> FsLayer::lookup(inode_id parent, const std::string& name) {
  /* TODO Lab1A: Your code here */
  auto entries = read_dir_entries(parent);
  if (!entries) return std::unexpected(entries.error());

  for (const auto& entry : *entries) {
    if (std::strncmp(entry.name, name.c_str(), NANOFS_NAME_MAX) == 0)
      return entry.inode_id;
  }
  return std::unexpected(Error::NotFound);
}

// Provided: public entry for read_dir_entries.
Result<std::vector<DirEntry>> FsLayer::readdir(inode_id dir) {
  return read_dir_entries(dir);
}

Result<void> FsLayer::add_dir_entry(inode_id dir, const std::string& name,
                                    inode_id target) {
  /* TODO Lab1A: Your code here */
  //* 0. validate the name and target
  if (name.size() >= NANOFS_NAME_MAX || target == 0)
    return std::unexpected(Error::Invalid);

  auto node = inodes_.read_inode(dir); //get the parent directory inode
  if (!node) return std::unexpected(node.error());
  if (node->type != INODE_DIR)
    return std::unexpected(Error::NotDir);

  //* 1. reject duplicate names and find a reusable slot
  auto raw = read_bytes(*node, 0, node->size); //node,offset,length
  if (!raw) return std::unexpected(raw.error());

  uint32_t slot = std::numeric_limits<uint32_t>::max();
  for (uint32_t off = 0; off + sizeof(DirEntry) <= raw->size();
       off += sizeof(DirEntry)) {
    DirEntry entry{}; 
    //?Directory entry: fixed 64 bytes. inode_id == 0 marks a free slot;
    //?name is NUL-terminated (<= 59 chars).
    std::memcpy(&entry, raw->data() + off, sizeof(entry));

    // reject dup name
    if (entry.inode_id != 0 &&
        std::strncmp(entry.name, name.c_str(), NANOFS_NAME_MAX) == 0)
      return std::unexpected(Error::Exists);
    
    // record the first free slot (when deleting, only inode_id will be set 0)
    if (entry.inode_id == 0 && slot == std::numeric_limits<uint32_t>::max())
      slot = off;
  }

  //* 2. append when no free slot exists
  if (slot == std::numeric_limits<uint32_t>::max())
    slot = node->size;//move slot to the tail

  // build the new entry
  DirEntry entry{};
  entry.inode_id = target;
  std::memcpy(entry.name, name.data(), name.size());
  // encode entry to bytes
  std::string encoded(sizeof(entry), '\0');
  std::memcpy(encoded.data(), &entry, sizeof(entry));

  // write back the entry bytes
  if (auto r = write_bytes(dir, *node, slot, encoded); !r)
    return std::unexpected(r.error());
  return {};
}

Result<void> FsLayer::remove_dir_entry(inode_id dir, const std::string& name) {
  /* TODO Lab1A: Your code here */
  //? removing an entry does not shrink the dir file
  //* 0. validate the name and target
  if (name.size() >= NANOFS_NAME_MAX )
    return std::unexpected(Error::Invalid);

  auto node = inodes_.read_inode(dir); //get the parent directory inode
  if (!node) return std::unexpected(node.error());
  if (node->type != INODE_DIR)
    return std::unexpected(Error::NotDir);

  //* 1. find the entry
  auto raw = read_bytes(*node, 0, node->size); //node,offset,length
  if (!raw) return std::unexpected(raw.error());

  uint32_t slot = std::numeric_limits<uint32_t>::max();

  for (uint32_t off = 0; off + sizeof(DirEntry) <= raw->size();
       off += sizeof(DirEntry)) {
    DirEntry entry{}; 
    std::memcpy(&entry, raw->data() + off, sizeof(entry));

    // find the name
    if (entry.inode_id != 0 &&
      std::strncmp(entry.name, name.c_str(), NANOFS_NAME_MAX) == 0){
        slot = off;
        break;
      }
  }

// no wanted entry: unlink is idempotent
  if (slot == std::numeric_limits<uint32_t>::max())
    return {};

  DirEntry entry{};
  entry.inode_id = 0;
  std::string encoded(sizeof(entry), '\0');
  std::memcpy(encoded.data(), &entry, sizeof(entry));

  //*2.write back the entry bytes
  if (auto r = write_bytes(dir, *node, slot, encoded); !r)
    return std::unexpected(r.error());

  return {};
}

// TODO Lab2B: keep this operation inside its existing parent lock; avoid
// re-entering LockedFs through a virtual lookup.
Result<inode_id> FsLayer::make_node(inode_id parent, const std::string& name,
                                    uint32_t type) {
  /* TODO Lab1A: Your code here */
  //* 0. verify that the parent is a directory
  auto parent_node = inodes_.read_inode(parent);
  if (!parent_node) return std::unexpected(parent_node.error());
  if (parent_node->type != INODE_DIR)
    return std::unexpected(Error::NotDir);

  //* 1. an existing name is idempotent
  auto existing = lookup(parent, name);
  if (existing) return *existing;
  if (existing.error() != Error::NotFound)
    return std::unexpected(existing.error());

  //* 2. allocate the inode before linking it into the parent
  auto child = inodes_.allocate_inode(type);
  if (!child) return std::unexpected(child.error());

  //* 3. roll back the inode if adding the directory entry fails
  if (auto r = add_dir_entry(parent, name, *child); !r) {
    (void)inodes_.free_inode(*child);
    return std::unexpected(r.error());
  }

  return *child;
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
  //* 0. free direct data blocks
  for (uint32_t i = 0; i < NDIRECT; ++i) {
    if (node.blocks[i] == NULL_BLOCK) continue;
    if (auto r = balloc.free(node.blocks[i]); !r)
      return std::unexpected(r.error());
    node.blocks[i] = NULL_BLOCK;
  }

  const uint32_t p = bs / sizeof(block_id);

  //* 1. free the single-indirect table and all data blocks it references
  if (node.blocks[NDIRECT] != NULL_BLOCK) {
    const block_id table = node.blocks[NDIRECT];
    auto ptrs = read_ptrs(dev_, table);
    if (!ptrs) return std::unexpected(ptrs.error());

    for (block_id data : *ptrs) {
      if (data == NULL_BLOCK) continue;
      if (auto r = balloc.free(data); !r)
        return std::unexpected(r.error());
    }
    if (auto r = balloc.free(table); !r)
      return std::unexpected(r.error());
    node.blocks[NDIRECT] = NULL_BLOCK;
  }

  //* 2. free the double-indirect tree
  if (node.blocks[NDIRECT + 1] != NULL_BLOCK) {
    const block_id root = node.blocks[NDIRECT + 1];
    auto root_ptrs = read_ptrs(dev_, root);
    if (!root_ptrs) return std::unexpected(root_ptrs.error());

    for (block_id table : *root_ptrs) {
      if (table == NULL_BLOCK) continue;
      auto ptrs = read_ptrs(dev_, table);
      if (!ptrs) return std::unexpected(ptrs.error());

      for (block_id data : *ptrs) {
        if (data == NULL_BLOCK) continue;
        if (auto r = balloc.free(data); !r)
          return std::unexpected(r.error());
      }
      if (auto r = balloc.free(table); !r)
        return std::unexpected(r.error());
    }

    if (auto r = balloc.free(root); !r)
      return std::unexpected(r.error());
    node.blocks[NDIRECT + 1] = NULL_BLOCK;
  }

  //* 3. reset the inode's file state
  node.size = 0;
  return {};
}

// TODO Lab2B: avoid re-entering LockedFs for this internal lookup.
Result<void> FsLayer::unlink(inode_id parent, const std::string& name) {
  /* TODO Lab1A: Your code here */
  // remove a file's directory entry and reclaim its blocks and inode
  //* 0. find the target; missing entries are idempotent
  auto target = lookup(parent, name);
  if (!target) {
    if (target.error() == Error::NotFound) return {};
    return std::unexpected(target.error());
  }

  //* 1. read and validate the target inode
  auto node = inodes_.read_inode(*target);
  if (!node) return std::unexpected(node.error());
  if (node->type == INODE_DIR)
    return std::unexpected(Error::IsDir);

  //* 2. remove the directory entry
  if (auto r = remove_dir_entry(parent, name); !r)
    return std::unexpected(r.error());

  //* 3. prepare the block allocator
  auto sb = super();
  if (!sb) return std::unexpected(sb.error());
  const uint32_t bs = static_cast<uint32_t>(dev_.block_size());
  BlockAllocator balloc(dev_, sb->block_bitmap_start,
                        sb->block_bitmap_blocks, sb->data_start,
                        sb->num_blocks);

  //* 4. reclaim content and indirect blocks
  if (auto r = free_file_blocks(*node, balloc, bs); !r)
    return std::unexpected(r.error());

  //* 5. persist the cleared inode and free its inode slot
  if (auto r = inodes_.write_inode(*target, *node); !r)
    return std::unexpected(r.error());
  if (auto r = inodes_.free_inode(*target); !r)
    return std::unexpected(r.error());

  return {};
}

// TODO Lab2B: avoid re-entering LockedFs for this internal lookup.
Result<void> FsLayer::rmdir(inode_id parent, const std::string& name) {
  /* TODO Lab1A: Your code here */
  //* 0. find the directory; missing entries are idempotent
  auto dir_id_result = lookup(parent, name);
  if (!dir_id_result) {
    if (dir_id_result.error() == Error::NotFound) return {};
    return std::unexpected(dir_id_result.error());
  }
  inode_id dir_id = *dir_id_result;

  //* 1. check that the target is a directory and is empty
  auto dir_node = inodes_.read_inode(dir_id); //! 取出来操作的实体node，最终写入这个
  if (!dir_node) return std::unexpected(dir_node.error());
  if (dir_node->type != INODE_DIR)
    return std::unexpected(Error::NotDir);

  auto entries = read_dir_entries(dir_id);
  if (!entries) return std::unexpected(entries.error());
  if (!entries->empty())
    return std::unexpected(Error::NotEmpty);

  //* 2. remove the parent directory entry
  if (auto r = remove_dir_entry(parent, name); !r)
    return std::unexpected(r.error());

  //* 3. prepare the block allocator
  auto sb = super();
  if (!sb) return std::unexpected(sb.error());
  const uint32_t bs = static_cast<uint32_t>(dev_.block_size());
  BlockAllocator balloc(dev_, sb->block_bitmap_start,
                        sb->block_bitmap_blocks, sb->data_start,
                        sb->num_blocks);

  //* 4. reclaim the directory's content and indirect blocks
  if (auto r = free_file_blocks(*dir_node, balloc, bs); !r)
    return std::unexpected(r.error());

  //* 5. persist the cleared inode and free its inode slot
  if (auto r = inodes_.write_inode(dir_id, *dir_node); !r)
    return std::unexpected(r.error());
  if (auto r = inodes_.free_inode(dir_id); !r)
    return std::unexpected(r.error());

  return {};
}

Result<Attr> FsLayer::setattr(inode_id id, uint32_t new_size) {
  /* TODO Lab1A: Your code here */
  //* 0. read inode
  auto node = inodes_.read_inode(id);
  const uint32_t bs = static_cast<uint32_t>(dev_.block_size());
  if (!node) return std::unexpected(node.error());

  if (node->type == INODE_DIR)
    return std::unexpected(Error::IsDir);

  if (new_size == node->size){
    Attr attr{
        .inode_id = id,
        .is_dir = node->type == INODE_DIR,
        .size = node->size,
    };

    return attr;
  }

  //*1. prepare allocator
  auto sb = super();
  if (!sb) return std::unexpected(sb.error());

  BlockAllocator balloc(
      dev_,
      sb->block_bitmap_start,
      sb->block_bitmap_blocks,
      sb->data_start,
      sb->num_blocks);

  //*2. handle different length

  //*2.1 shrink
  if (new_size < node->size) {
    const uint32_t old_size = node->size;
    const uint32_t tail_idx =
        (new_size + bs - 1) / bs;
    const uint32_t p = bs / sizeof(block_id);

    // TODO
    // - 保留 [0, ceil(new_size / bs)) 的逻辑块
    // - 释放之后的逻辑块
    // - 清理不再需要的 indirect blocks
    // - 清零最后一个保留块的 EOF 后半部分

    //*2.1.1 direct blocks
    const uint32_t direct_end = std::min(tail_idx, NDIRECT);
    for (uint32_t i = direct_end; i < NDIRECT; ++i) {
      if (node->blocks[i] != NULL_BLOCK) {
        if (auto r = balloc.free(node->blocks[i]); !r)
          return std::unexpected(r.error());
        node->blocks[i] = NULL_BLOCK;
      }
    }

    //*2.1.2 single-indirect blocks
    const block_id single_block = node->blocks[NDIRECT];
    if (single_block != NULL_BLOCK) {
      auto single_ptrs = read_ptrs(dev_, single_block);
      if (!single_ptrs)
        return std::unexpected(single_ptrs.error());

      uint32_t keep_single = 0;
      if (tail_idx > NDIRECT)
        keep_single = std::min(tail_idx - NDIRECT, p);

      for (uint32_t i = keep_single; i < p; ++i) {
        if ((*single_ptrs)[i] != NULL_BLOCK) {
          if (auto r = balloc.free((*single_ptrs)[i]); !r)
            return std::unexpected(r.error());
          (*single_ptrs)[i] = NULL_BLOCK;
        }
      }

      if (keep_single == 0) {
        if (auto r = balloc.free(single_block); !r)
          return std::unexpected(r.error());
        node->blocks[NDIRECT] = NULL_BLOCK;
      } else {
        if (auto r = write_ptrs(dev_, single_block, *single_ptrs); !r)
          return std::unexpected(r.error());
      }
    }

    //*2.1.3 double-indirect blocks
    const block_id double_block = node->blocks[NDIRECT + 1];
    if (double_block != NULL_BLOCK) {
      auto double_ptrs = read_ptrs(dev_, double_block);
      if (!double_ptrs)
        return std::unexpected(double_ptrs.error());

      uint32_t keep_double = 0;
      if (tail_idx > NDIRECT + p) {
        keep_double =
            std::min(tail_idx - NDIRECT - p, p * p);
      }

      const uint32_t keep_tables =
          (keep_double + p - 1) / p;

      for (uint32_t i = keep_tables; i < p; ++i) {
        if ((*double_ptrs)[i] == NULL_BLOCK)
          continue;

        auto table_ptrs = read_ptrs(dev_, (*double_ptrs)[i]);
        if (!table_ptrs)
          return std::unexpected(table_ptrs.error());

        for (block_id data_block : *table_ptrs) {
          if (data_block != NULL_BLOCK) {
            if (auto r = balloc.free(data_block); !r)
              return std::unexpected(r.error());
          }
        }

        if (auto r = balloc.free((*double_ptrs)[i]); !r)
          return std::unexpected(r.error());

        (*double_ptrs)[i] = NULL_BLOCK;
      }

      if (keep_tables > 0) {
        const uint32_t last_table = keep_tables - 1;
        const uint32_t keep_in_last =
            keep_double - last_table * p;

        if (keep_in_last < p &&
            (*double_ptrs)[last_table] != NULL_BLOCK) {
          auto table_ptrs =
              read_ptrs(dev_, (*double_ptrs)[last_table]);
          if (!table_ptrs)
            return std::unexpected(table_ptrs.error());

          for (uint32_t i = keep_in_last; i < p; ++i) {
            if ((*table_ptrs)[i] != NULL_BLOCK) {
              if (auto r = balloc.free((*table_ptrs)[i]); !r)
                return std::unexpected(r.error());
              (*table_ptrs)[i] = NULL_BLOCK;
            }
          }

          if (auto r = write_ptrs(
                  dev_, (*double_ptrs)[last_table], *table_ptrs);
              !r)
            return std::unexpected(r.error());
        }
      }

      if (keep_tables == 0) {
        if (auto r = balloc.free(double_block); !r)
          return std::unexpected(r.error());
        node->blocks[NDIRECT + 1] = NULL_BLOCK;
      } else {
        if (auto r = write_ptrs(dev_, double_block, *double_ptrs); !r)
          return std::unexpected(r.error());
      }
    }

    //*2.1.4 clear the unused tail of the last retained block
    if (new_size > 0 && new_size % bs != 0) {
      const uint32_t logical_idx = new_size / bs;
      auto physical = logical_to_physical_block(
          *node, logical_idx, false, balloc, bs);

      if (!physical)
        return std::unexpected(physical.error());

      if (*physical != NULL_BLOCK) {
        std::vector<std::byte> block(bs);

        if (auto r = dev_.read_block(*physical, block); !r)
          return std::unexpected(r.error());

        std::memset(
            block.data() + (new_size % bs),
            0,
            bs - (new_size % bs));

        if (auto r = dev_.write_block(
                *physical, block, BlockKind::Data);
            !r)
          return std::unexpected(r.error());
      }
    }

    node->size = new_size;

    if (auto r = inodes_.write_inode(id, *node); !r)
      return std::unexpected(r.error());

    return Attr{
        .inode_id = id,
        .is_dir = false,
        .size = node->size,
    };
  }

  //*2.2 grow
  //Growing a file changes its size without allocating the added range
  node->size = new_size;
  if (auto r = inodes_.write_inode(id, *node); !r) //?这里因为是一个Result，所以要解一下
    return std::unexpected(r.error());

  //*3. return
  Attr attr{
      .inode_id = id,
      .is_dir = node->type == INODE_DIR,
      .size = node->size,
  };


  return attr;
}
}  // namespace nanofs
