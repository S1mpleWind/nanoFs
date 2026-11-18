# Lab 3: Make nanoCoder history fault-tolerant

Lab 3 builds on your completed Lab 2 filesystem and adds a Raft-backed key-value
service for durable nanoCoder sessions. Complete the single implementation part:

- **[Lab 3 tasks](lab3-tasks.md)**: implement the Raft node and use the provided KV service
  to verify election, replication, persistence and snapshots.

The node, transport, storage, KV state machine, server, client and tests are
provided. Students edit only `nanoraft/src/raft.cc`.

## Get Lab 3 once

Commit your completed Lab 2 work, then merge the Lab 3 release into the same
working branch:

```sh
git add nanofs/src/fs.cc nanofs/src/inode.cc nanofs/src/handle.cc \
  nanofs/include/nanofs/remote_fs.h nanofs/src/journal.cc \
  nanofs/src/locked_fs.cc
git commit -m "Complete Lab2"
git fetch https://ipads.sjtu.edu.cn:1312/lab/cse-2026-fall.git lab3
git merge FETCH_HEAD
```

If the Lab 3 files are already present in your repository, skip the fetch and
merge. Keep your Lab 2 implementation when resolving any unrelated conflicts.
Do not start Lab 3 with uncommitted Lab 2 changes; the Lab 3 handin contains
only `nanoraft/src/raft.cc`.

## Development container

Reuse the Lab 1/Lab 2 Docker image and container. Lab 3's required build and
tests do not need FUSE, so a container without `/dev/fuse` is sufficient.
The repository is mounted at `/work`; run the commands below inside the
container from that directory.

If you already have the Lab 2 container, return to it with:

```sh
docker start -ai cse-2026-lab2-fuse
```

If you need a new container, pull or build the image first from the repository
root on the host:

```sh
docker pull chilsome/cse-2026-lab1:20260918
docker tag chilsome/cse-2026-lab1:20260918 cse-2026-lab3
```

Alternatively, build it locally:

```sh
docker build -t cse-2026-lab3 .
```

Start a container without FUSE support:

**macOS / WSL Ubuntu (Bash or Zsh):**

```sh
docker run --name cse-2026-lab3 -it \
  -e NANOFS_BUILD_DIR=/work/build-linux \
  -v "$PWD:/work" cse-2026-lab3
```

**Windows PowerShell:**

```powershell
docker run --name cse-2026-lab3 -it -e NANOFS_BUILD_DIR=/work/build-linux -v "${PWD}:/work" cse-2026-lab3
```

You can open another shell with:

```sh
docker exec -it cse-2026-lab3 bash
```

The C++ tests and the smoke test run inside the container.

## Grading and handin

Follow [Build, test, and grade](lab3-tasks.md#build-test-grade) for the test groups,
recommended coding order and integration test commands. Run
`scripts/lab3/test.sh` from `/work` before submitting.

After all required tests pass, create the single Lab 3 archive:

```sh
scripts/lab3/handin.sh YOUR_STUDENT_ID
```

The archive is `handin-lab3-YOUR_STUDENT_ID.tgz` and contains only:

```text
nanoraft/src/raft.cc
```

Inspect it before uploading:

```sh
tar -tzf handin-lab3-YOUR_STUDENT_ID.tgz
```

Keep the tests and all provided components unchanged.
