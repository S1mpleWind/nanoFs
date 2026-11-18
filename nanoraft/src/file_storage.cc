#include "nanoraft/file_storage.h"
#include <filesystem>
#include <fstream>
#include <iterator>
#include <utility>
#include <type_traits>       // libc++/C++23: pull in traits before msgpack (repo note)
#include <rpc/msgpack.hpp>

namespace nanoraft {

FileStorage::FileStorage(std::string path) : path_(std::move(path)) {}

void FileStorage::save(const PersistentState& state) {
  clmdep_msgpack::sbuffer sb;
  clmdep_msgpack::pack(sb, state);
  std::string tmp = path_ + ".tmp";
  {
    std::ofstream out(tmp, std::ios::binary | std::ios::trunc);
    out.write(sb.data(), static_cast<std::streamsize>(sb.size()));
  }  // close (flush) before rename so the replace sees complete bytes
  std::filesystem::rename(tmp, path_);   // atomic replace
}

PersistentState FileStorage::load() {
  std::ifstream in(path_, std::ios::binary);
  if (!in) return PersistentState{};
  std::string buf((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
  if (buf.empty()) return PersistentState{};
  auto oh = clmdep_msgpack::unpack(buf.data(), buf.size());
  return oh.get().as<PersistentState>();
}

}  // namespace nanoraft
