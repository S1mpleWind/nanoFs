#include "nanofs/journal.h"
#include "nanofs/log.h"
#include <cstring>

namespace nanofs {

namespace {
// FNV-1a over the bytes of every record block; detects a torn tail of a
// multi-block journal write.
[[maybe_unused]] uint32_t checksum_records(
    const std::vector<std::vector<std::byte>>& recs) {
  uint32_t h = 2166136261u;
  for (const auto& r : recs)
    for (std::byte b : r) {
      h ^= std::to_integer<uint32_t>(b);
      h *= 16777619u;
    }
  return h;
}
}  // namespace

// The transaction-map helpers are provided. Students implement the public
// journal operations below according to journal.h and docs/lab2/lab2a.md.
JournaledBlockDevice::WriteSet* JournaledBlockDevice::current_tx() {
  std::lock_guard<std::mutex> g(tx_map_mutex_);
  auto it = txs_.find(std::this_thread::get_id()); // no nesting is allowed
  return it == txs_.end() ? nullptr : &it->second;
}

void JournaledBlockDevice::erase_tx() {
  std::lock_guard<std::mutex> g(tx_map_mutex_);
  txs_.erase(std::this_thread::get_id());
}

bool JournaledBlockDevice::create_tx() {
  std::lock_guard<std::mutex> g(tx_map_mutex_);
  return txs_.try_emplace(std::this_thread::get_id()).second;
}

Result<void> JournaledBlockDevice::begin() {
  /* TODO Lab2A: Your code here */
  //* creates only one write set
  if (!create_tx()) return std::unexpected(Error::Invalid);
  return {};
}

Result<void> JournaledBlockDevice::abort() {
  /* TODO Lab2A: Your code here */
  if (current_tx() == nullptr) return std::unexpected(Error::Invalid);
  erase_tx();
  return {};
}

Result<void> JournaledBlockDevice::write_block(block_id id,
                                               std::span<const std::byte> buf,
                                               BlockKind kind) {
  /* TODO Lab2A: Your code here */
  //* 0. check range
  if (id >= image_.num_blocks() || buf.size() != image_.block_size())
    return std::unexpected(Error::Invalid);

  WriteSet* tx = current_tx();

  //* Data use no Journal
  if (kind == BlockKind::Data) {
    if (tx != nullptr) tx->erase(id);
    return image_.write_block(id, buf, kind);
  }

  //* Meta data uses journal, write in tx
  if (tx != nullptr) {
    (*tx)[id] = std::vector<std::byte>(buf.begin(), buf.end());
    return {};
  }

  return image_.write_block(id, buf, kind);
}


Result<void> JournaledBlockDevice::read_block(block_id id,
                                              std::span<std::byte> buf) {
  if (id >= image_.num_blocks() || buf.size() != image_.block_size())
    return std::unexpected(Error::Invalid);

  WriteSet* tx = current_tx();
  if (tx != nullptr) {
    auto it = tx->find(id);

    // vector的end就是没找到；it->second是byte的vector
    if (it != tx->end()) {
      std::copy(it->second.begin(), it->second.end(), buf.begin());
      return {};
    }
  }

  return image_.read_block(id, buf);
}

Result<void> JournaledBlockDevice::zero_block(block_id id) {
  if (id >= image_.num_blocks()) return std::unexpected(Error::Invalid);

  WriteSet* tx = current_tx();
  if (tx != nullptr) {
    (*tx)[id] = std::vector<std::byte>(image_.block_size(), std::byte{0});
    return {};
  }

  return image_.zero_block(id);
}

Result<void> JournaledBlockDevice::commit() {
  // commit 只能结束当前线程已经 begin 的事务。
  WriteSet* tx = current_tx();
  if (tx == nullptr) return std::unexpected(Error::Invalid);

  // 保护日志尾部、cursor_ 和提交过程，避免两个线程交错写日志。
  std::lock_guard<std::mutex> lock(journal_lock_);

  const size_t block_size = journal_.block_size();
  if (block_size < sizeof(JournalDesc)) {
    erase_tx();
    return std::unexpected(Error::Invalid);
  }

  const size_t max_records =
      (block_size - sizeof(JournalDesc)) / sizeof(block_id);
  const size_t n = tx->size();

  // 空事务没有内容需要记录，也不消耗日志块。
  if (n == 0) {
    erase_tx();
    return {};
  }

  if (n > max_records || n + 1 > journal_.num_blocks()) {
    erase_tx();
    return std::unexpected(Error::NoSpace);
  }

  const block_id needed = static_cast<block_id>(1 + n);
  // 当前日志空间不够时，先把已提交内容写入 image 并清空日志。
  if (static_cast<size_t>(cursor_) + needed > journal_.num_blocks()) {
    if (auto r = checkpoint(); !r) {
      erase_tx();
      return r;
    }
  }

  std::vector<std::vector<std::byte>> records;
  std::vector<block_id> targets;
  records.reserve(n);
  targets.reserve(n);

  for (const auto& [id, data] : *tx) {
    // descriptor 中的目标块编号和 records 中的数据按相同顺序排列。
    targets.push_back(id);
    records.push_back(data);
  }

  // 先写每个块的新内容；descriptor 记录这些块的目标位置。
  for (size_t i = 0; i < n; ++i) {
    if (auto r = journal_.write_block(
            cursor_ + 1 + static_cast<block_id>(i), records[i]); !r) {
      erase_tx();
      return r;
    }
  }

  JournalDesc desc{
      NANOFS_JRNL_MAGIC,
      static_cast<uint32_t>(n),
      checksum_records(records),
  };

  std::vector<std::byte> desc_block(block_size, std::byte{0});
  // descriptor 头之后紧接着存放目标 image 块编号数组。
  std::memcpy(desc_block.data(), &desc, sizeof(desc));
  std::memcpy(desc_block.data() + sizeof(desc), targets.data(),
              targets.size() * sizeof(block_id));

  if (auto r = journal_.write_block(cursor_, desc_block); !r) {
    erase_tx();
    return r;
  }

  // 日志 flush 成功是提交点：此前失败可报告事务未提交；此后不能再报告失败。
  if (auto r = journal_.flush(); !r) {
    erase_tx();
    return r;
  }

  cursor_ += needed;

  // 到这里事务已经提交。把日志内容应用到 image 的易失缓存，但不在这里 flush。
  // 若 image 写入失败，保留已提交日志供下次 recovery 重做，不能报告事务失败。
  for (size_t i = 0; i < n; ++i) {
    if (auto r = image_.write_block(targets[i], records[i]); !r) {
      NANOFS_LOG_ERROR("applying committed journal transaction failed: "
                       << to_string(r.error()));
      break;
    }
  }

  if (cursor_ >= checkpoint_threshold_) {
    // 日志接近阈值时 checkpoint，避免后续事务没有空间。
    if (auto r = checkpoint(); !r) {
      NANOFS_LOG_ERROR("checkpoint after commit failed: "
                       << to_string(r.error()));
    }
  }

  erase_tx();
  return {};
}

Result<void> JournaledBlockDevice::reset_journal() {
  std::vector<std::byte> zero(journal_.block_size(), std::byte{0});

  //* 循环写回0,用于reset
  for (size_t id = 0; id < journal_.num_blocks(); ++id) {
    if (auto r = journal_.write_block(
            static_cast<block_id>(id), zero); !r) {
      return r;
    }
  }

  //* flush回disk
  if (auto r = journal_.flush(); !r) return r;

  cursor_ = 0;
  return {};
}

Result<void> JournaledBlockDevice::checkpoint() {
  //* 1.持久化image
  if (auto r = image_.flush(); !r) return r;

  //* 2.清空journal
  return reset_journal();
}

Result<void> JournaledBlockDevice::recover() {
  //* 0.上锁，恢复期间不能与 commit 同时读写日志尾部
  std::lock_guard<std::mutex> lock(journal_lock_);

  const size_t block_size = journal_.block_size();
  if (block_size < sizeof(JournalDesc))
    return std::unexpected(Error::Invalid);

  const size_t max_records =
      (block_size - sizeof(JournalDesc)) / sizeof(block_id);

  size_t pos = 0;
  bool replayed = false;

  //* 整体循环
  //日志按 [descriptor][record...][descriptor][record...] 顺序排列，从头扫描。
  while (pos < journal_.num_blocks()) {
    std::vector<std::byte> desc_block(block_size);

    // 设备读取错误不是损坏日志尾部；返回错误并保留 journal，避免丢失可恢复事务。
    if (auto r = journal_.read_block(
            static_cast<block_id>(pos), desc_block); !r) {
      return r;  // 读取错误不能当作日志尾部无效，也不能清日志
    }

    JournalDesc desc{};
    std::memcpy(&desc, desc_block.data(), sizeof(desc));

    // magic 不匹配或空记录数表示没有有效事务，扫描到日志尾部
    if (desc.magic != NANOFS_JRNL_MAGIC || desc.n_records == 0)
      break;

    const size_t n = desc.n_records;
    // 防止 descriptor 描述的记录越过自身块的容量或 journal 设备末尾。
    if (n > max_records || n > journal_.num_blocks() - pos - 1)
      break;

    //* 1.取出每个 record 对应的 image 目标块编号
    std::vector<block_id> targets(n);
    std::memcpy(targets.data(), desc_block.data() + sizeof(desc),
                n * sizeof(block_id));

    std::vector<std::vector<std::byte>> records;
    records.reserve(n);

    bool read_failed = false;
    Error read_error = Error::IoError;

    //* 2.读取事务的所有 record；
    // 读失败要保留日志，不能当作普通 torn tail。
    for (size_t i = 0; i < n; ++i) {
      std::vector<std::byte> record(block_size);
      auto r = journal_.read_block(
          static_cast<block_id>(pos + 1 + i), record);
      if (!r) {
        read_failed = true;
        read_error = r.error();
        break;
      }
      records.push_back(std::move(record));
    }

    if (read_failed) return std::unexpected(read_error);

    //* 3.checksum校验
    // 不匹配说明事务记录不完整；此事务及后续内容都不重放。
    if (checksum_records(records) != desc.records_checksum)
      break;

    //* 4,redo，写回对应的位置
    // 校验通过后按 descriptor 指定的位置 redo。整块覆盖使重复恢复安全。
    for (size_t i = 0; i < n; ++i) {
      if (targets[i] >= image_.num_blocks())
        return std::unexpected(Error::Invalid);

      if (auto r = image_.write_block(targets[i], records[i]); !r)
        return r;
    }

    replayed = true;
    // 跳过当前 descriptor 和它后面的 n 个 record，继续找下一笔事务。
    pos += 1 + n;
  }

  //* 5.重放结果先持久化，再清除日志；
  // 否则断电后可能既没有 image 更新也没有日志。
  if (replayed) {
    if (auto r = image_.flush(); !r) return r;
  }

  return reset_journal();
}

}  // namespace nanofs
