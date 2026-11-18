#include "nanoraft/file_storage.h"
#include <gtest/gtest.h>
#include <filesystem>

using namespace nanoraft;

// A saved PersistentState round-trips through a fresh FileStorage on the same path.
TEST(FileStorage, RoundTripsAcrossReopen) {
  auto path = std::filesystem::temp_directory_path() / "nanoraft_fs_roundtrip.bin";
  std::filesystem::remove(path);
  {
    FileStorage s(path.string());
    PersistentState st;
    st.current_term = 7;
    st.voted_for = 2;
    st.log = {LogEntry{7, "a"}, LogEntry{7, "b"}};
    st.snapshot_index = 3;
    st.snapshot_term = 5;
    st.snapshot = "snapbytes";
    s.save(st);
  }
  FileStorage s2(path.string());          // reopen: outlives the first FileStorage
  PersistentState got = s2.load();
  EXPECT_EQ(got.current_term, 7);
  EXPECT_EQ(got.voted_for, 2);
  ASSERT_EQ(got.log.size(), 2u);
  EXPECT_EQ(got.log[1].command, "b");
  EXPECT_EQ(got.snapshot_index, 3);
  EXPECT_EQ(got.snapshot_term, 5);
  EXPECT_EQ(got.snapshot, "snapbytes");
  std::filesystem::remove(path);
}

// An absent file loads the default PersistentState (fresh node).
TEST(FileStorage, AbsentPathLoadsDefaults) {
  auto path = std::filesystem::temp_directory_path() / "nanoraft_fs_absent.bin";
  std::filesystem::remove(path);
  FileStorage s(path.string());
  PersistentState got = s.load();
  EXPECT_EQ(got.current_term, 0);
  EXPECT_EQ(got.voted_for, -1);
  EXPECT_TRUE(got.log.empty());
  EXPECT_EQ(got.snapshot_index, 0);
}
