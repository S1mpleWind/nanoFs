# nanoraft — C++ Raft (Lab 3). Sibling to nanofs; shares librpc + GTest.

# src/raft.cc is the only file students fill in; every other source here is
# provided (see the header comment in each).
add_library(nanoraft_core
  src/raft.cc
  src/raft_scaffold.cc
  src/kv_state_machine.cc
  src/kv_codec.cc
  src/file_storage.cc
  src/librpc_network.cc)
target_include_directories(nanoraft_core PUBLIC include)
target_link_libraries(nanoraft_core PUBLIC librpc)
find_package(Threads REQUIRED)
target_link_libraries(nanoraft_core PUBLIC Threads::Threads)
target_compile_features(nanoraft_core PUBLIC cxx_std_23)

# nanoraftd — the fault-tolerant KV service node (two RPC planes on one server).
add_executable(nanoraftd server/main.cc)
target_link_libraries(nanoraftd PRIVATE nanoraft_core)

# nanoraft_kvctl — tiny KV client (leader redirect) for smoke/manual testing.
add_executable(nanoraft_kvctl client/kvctl.cc)
target_link_libraries(nanoraft_kvctl PRIVATE nanoraft_core)

add_executable(test_raft_smoke test/test_smoke.cc)
target_link_libraries(test_raft_smoke PRIVATE nanoraft_core GTest::gtest_main)
add_test(NAME RaftSmoke COMMAND test_raft_smoke)

add_executable(test_raft_election test/test_raft_election.cc)
target_include_directories(test_raft_election PRIVATE test)
target_link_libraries(test_raft_election PRIVATE nanoraft_core GTest::gtest_main)
add_test(NAME RaftElection COMMAND test_raft_election)

add_executable(test_raft_replication test/test_raft_replication.cc)
target_include_directories(test_raft_replication PRIVATE test)
target_link_libraries(test_raft_replication PRIVATE nanoraft_core GTest::gtest_main)
add_test(NAME RaftAppendEntries COMMAND test_raft_replication
  --gtest_filter=RaftAppendEntries.*)
add_test(NAME RaftReplication COMMAND test_raft_replication
  --gtest_filter=RaftReplication.*)

add_executable(test_raft_persist test/test_raft_persist.cc)
target_include_directories(test_raft_persist PRIVATE test)
target_link_libraries(test_raft_persist PRIVATE nanoraft_core GTest::gtest_main)
add_test(NAME RaftPersist COMMAND test_raft_persist)

add_executable(test_raft_snapshot test/test_raft_snapshot.cc)
target_include_directories(test_raft_snapshot PRIVATE test)
target_link_libraries(test_raft_snapshot PRIVATE nanoraft_core GTest::gtest_main)
add_test(NAME RaftInstallSnapshot COMMAND test_raft_snapshot
  --gtest_filter=RaftInstallSnapshot.*)
add_test(NAME RaftSnapshot COMMAND test_raft_snapshot
  --gtest_filter=RaftSnapshot.*)

add_executable(test_kv_state_machine test/test_kv_state_machine.cc)
target_include_directories(test_kv_state_machine PRIVATE test)
target_link_libraries(test_kv_state_machine PRIVATE nanoraft_core GTest::gtest_main)
add_test(NAME KvStateMachine COMMAND test_kv_state_machine
  --gtest_filter=KvStateMachine.*)
add_test(NAME RaftKv COMMAND test_kv_state_machine
  --gtest_filter=RaftKv.*)

add_executable(test_file_storage test/test_file_storage.cc)
target_link_libraries(test_file_storage PRIVATE nanoraft_core GTest::gtest_main)
add_test(NAME FileStorage COMMAND test_file_storage)

# The six entries above select a suite out of a shared binary with --gtest_filter.
# A filter that matches nothing exits 0 ("0 tests ... PASSED"), so a renamed or
# deleted suite would silently score green. Fail those entries instead.
set_tests_properties(RaftAppendEntries RaftReplication RaftInstallSnapshot
  RaftSnapshot KvStateMachine RaftKv
  PROPERTIES FAIL_REGULAR_EXPRESSION "0 tests from 0 test suites ran")
