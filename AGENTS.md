# Repository Guidelines

## Project Structure & Module Organization
- `nanofs/` contains the C++23 filesystem: public headers in `include/nanofs/`, implementations in `src/`, the FUSE entry point in `fuse-adaptor/`, unit tests in `test/`, and stress helpers in `stress/`.
- `nanocoder/` contains the Python coding-assistant application. `docs/lab1/` holds the lab and architecture guides, while `scripts/lab1/` contains build, integration, mount, and hand-in helpers.
- `examples/` contains sample inputs; `third_party/googletest/` is vendored. Treat `build*/` directories as generated output.

## Build, Test, and Development Commands
Use the Docker environment described in `docs/lab1/lab1.md` when possible. From the repository root:

```sh
cmake -S . -B build -DNANOFS_STAGE_B=OFF
cmake --build build -j4
bash scripts/lab1/test-a.sh
ctest --test-dir build -R '^LogicalToPhysical\.' --output-on-failure
scripts/lab1/integration_test.sh a quick
scripts/lab1/integration_test.sh a stress --seed 1
```

The first two commands configure and compile. `test-a.sh` runs the Lab1A unit suite; `ctest` is useful for one focused suite. Integration tests exercise the FUSE-mounted filesystem. Set `NANOFS_BUILD_DIR` to use another build directory, such as `build-linux`.

## Coding Style & Naming Conventions
Use two-space indentation in C++ and four spaces in Python; keep continuation lines aligned with surrounding code. C++ types use `PascalCase`, functions and files use `snake_case`, and constants use the existing uppercase style. Python modules, functions, and variables use `snake_case`. No repository-wide formatter or linter is configured, so preserve nearby style and keep public interfaces documented in headers.

## Testing Guidelines
Add C++ unit tests under `nanofs/test/test_<component>.cc` using GoogleTest and descriptive suite/test names. Cover normal behavior, invalid inputs, persistence, and boundary cases. There is no configured coverage threshold; run the full unit script and relevant integration tests before submitting.

## Commit & Pull Request Guidelines
Use short, imperative commit subjects (for example, `nanofs: fix inode allocation`), matching the concise style in history. Pull requests should explain the behavior change, list test commands and results, link any related issue or lab task, and include screenshots or mount/reproduction steps when changing user-visible FUSE behavior. Do not commit build directories, disk images, logs, or hand-in archives.
