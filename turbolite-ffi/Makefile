# turbolite-ffi build targets
#
# Two cdylib flavors:
#   make lib           — standalone .dylib/.so for C/Python/Go/Node FFI
#                        (bundled SQLite)
#   make ext           — loadable SQLite extension (.dylib/.so) dlopened
#                        by an existing sqlite3 host

UNAME := $(shell uname -s)
ifeq ($(UNAME),Darwin)
  LIB_EXT := dylib
  LIB_PREFIX := lib
else
  LIB_EXT := so
  LIB_PREFIX := lib
endif

LIB_NAME := turbolite_ffi
LIB_FILE := $(LIB_PREFIX)$(LIB_NAME).$(LIB_EXT)
# turbolite is a Cargo workspace; its .cargo/config.toml points at
# `target-dir = "../cinch-target"` relative to the workspace root,
# resolving to ~/Documents/Github/cinch-target. From this crate's
# directory that's two levels up.
CARGO_TARGET_DIR ?= ../../cinch-target
TARGET_DIR := $(CARGO_TARGET_DIR)/release

# Features forwarded to cargo. Override with: make lib FEATURES="zstd,encryption"
FEATURES ?= zstd

# ── Shared library ─────────────────────────────────────────────────

.PHONY: lib
lib: ## Build standalone cdylib with bundled SQLite (for C / Python / Go / Node FFI)
	cargo build --release --lib --no-default-features --features bundled-sqlite,$(FEATURES)
	@echo ""
	@echo "Built: $(TARGET_DIR)/$(LIB_FILE)"
	@ls -lh $(TARGET_DIR)/$(LIB_FILE)

# ── Loadable extension ────────────────────────────────────────────

EXT_FEATURES ?= zstd,cli-s3

.PHONY: ext
ext: ## Build loadable extension (default: with S3 cli-s3 feature)
	cargo build --release --lib --no-default-features --features loadable-extension,$(EXT_FEATURES)
	@cp $(TARGET_DIR)/$(LIB_FILE) $(TARGET_DIR)/turbolite.$(LIB_EXT)
	@echo ""
	@echo "Built loadable extension: $(TARGET_DIR)/turbolite.$(LIB_EXT)"
	@ls -lh $(TARGET_DIR)/turbolite.$(LIB_EXT)

.PHONY: ext-local
ext-local: ## Build loadable extension (local compression only, no S3)
	cargo build --release --lib --no-default-features --features loadable-extension,zstd
	@cp $(TARGET_DIR)/$(LIB_FILE) $(TARGET_DIR)/turbolite.$(LIB_EXT)
	@echo ""
	@echo "Built loadable extension (local only): $(TARGET_DIR)/turbolite.$(LIB_EXT)"
	@ls -lh $(TARGET_DIR)/turbolite.$(LIB_EXT)

# ── C header ───────────────────────────────────────────────────────

.PHONY: header
header: ## Generate turbolite.h C header via cbindgen
	cbindgen --config cbindgen.toml --crate turbolite-ffi --output turbolite.h
	@echo "Generated: turbolite.h"

# ── Install ────────────────────────────────────────────────────────

PREFIX ?= /usr/local

.PHONY: install
install: lib header ## Install shared library + header to PREFIX (default /usr/local)
	install -d $(PREFIX)/lib $(PREFIX)/include
	install -m 755 $(TARGET_DIR)/$(LIB_FILE) $(PREFIX)/lib/
	install -m 644 turbolite.h $(PREFIX)/include/
	@echo ""
	@echo "Installed to $(PREFIX)/lib/$(LIB_FILE) and $(PREFIX)/include/turbolite.h"

# ── Tests ──────────────────────────────────────────────────────────

.PHONY: test
test: ## Run Rust-level FFI integration tests
	cargo test --features zstd,bundled-sqlite

.PHONY: test-auto-ext-python
test-auto-ext-python: ext ## Python e2e: loadable-ext + sqlite3_auto_extension
	/opt/homebrew/bin/python3 tests/test_auto_extension_python.py

# FFI integration test matrix. Each target builds the correct cdylib
# flavor then drives it from the language in question. The legacy
# tests/test_ffi_* files exercise the standalone-cdylib C-level symbol
# surface; test-ext exercises the loadable-extension path.

.PHONY: test-ext
test-ext: ext ## Loadable extension tests from Python (requires Homebrew Python)
	/opt/homebrew/bin/python3 tests/test_loadable_ext.py

.PHONY: test-ffi
test-ffi: ## Run all standalone-cdylib FFI tests (builds once with cli-s3 + zstd)
	$(MAKE) lib FEATURES="zstd,cli-s3"
	$(MAKE) test-ffi-python-only test-ffi-c-only test-ffi-go-only test-ffi-node-only

# Per-language targets without their own `lib` dependency — let the
# caller do one cdylib build. Use these when you already ran `make lib`.
.PHONY: test-ffi-python-only
test-ffi-python-only:
	python3 tests/test_ffi_python.py

.PHONY: test-ffi-c-only
test-ffi-c-only:
	$(MAKE) header
	cc -o $(TARGET_DIR)/test_ffi_c tests/test_ffi_c.c \
		-DTURBOLITE_TIERED -DTURBOLITE_CLOUD \
		-L$(TARGET_DIR) -lturbolite_ffi \
		-Wl,-rpath,$(CURDIR)/$(TARGET_DIR)
	$(TARGET_DIR)/test_ffi_c

.PHONY: test-ffi-go-only
test-ffi-go-only:
	cd tests/test_ffi_go && \
		DYLD_LIBRARY_PATH=$(CURDIR)/$(TARGET_DIR) \
		CGO_LDFLAGS="-L$(CURDIR)/$(TARGET_DIR) -lturbolite_ffi" \
		go run .

.PHONY: test-ffi-node-only
test-ffi-node-only:
	cd tests/test_ffi_node && npm install --silent 2>/dev/null && node test.mjs

.PHONY: test-ffi-tiered
test-ffi-tiered: ## FFI tests with tiered/S3 (needs S3 creds)
	$(MAKE) test-ffi FEATURES="zstd,cli-s3"

.PHONY: test-ffi-python
test-ffi-python: lib ## Python ctypes
	python3 tests/test_ffi_python.py

# Build CFLAGS that mirror Cargo features for the C test.
FFI_C_DEFINES :=
ifneq (,$(findstring cli-s3,$(FEATURES)))
  FFI_C_DEFINES += -DTURBOLITE_TIERED
endif
ifneq (,$(findstring encryption,$(FEATURES)))
  FFI_C_DEFINES += -DTURBOLITE_ENCRYPTION
endif

.PHONY: test-ffi-c
test-ffi-c: lib header ## C (proves turbolite.h works)
	cc -o $(TARGET_DIR)/test_ffi_c tests/test_ffi_c.c \
		$(FFI_C_DEFINES) \
		-L$(TARGET_DIR) -lturbolite_ffi \
		-Wl,-rpath,$(CURDIR)/$(TARGET_DIR)
	$(TARGET_DIR)/test_ffi_c

.PHONY: test-ffi-go
test-ffi-go: lib ## Go cgo
	cd tests/test_ffi_go && \
		DYLD_LIBRARY_PATH=$(CURDIR)/$(TARGET_DIR) \
		CGO_LDFLAGS="-L$(CURDIR)/$(TARGET_DIR) -lturbolite_ffi" \
		go run .

.PHONY: test-ffi-node
test-ffi-node: lib ## Node.js koffi
	cd tests/test_ffi_node && npm install --silent 2>/dev/null && node test.mjs

# ── Language packages ─────────────────────────────────────────────

.PHONY: pkg-python
pkg-python: ext ## Build Python package (builds ext, bundles binary, creates wheel)
	cp $(TARGET_DIR)/turbolite.$(LIB_EXT) packages/python/turbolite/
	cd packages/python && pip wheel . --no-deps -w dist/

.PHONY: pkg-python-dev
pkg-python-dev: ext ## Install Python package in dev mode (builds ext first)
	cp $(TARGET_DIR)/turbolite.$(LIB_EXT) packages/python/turbolite/
	cd packages/python && pip install -e .

.PHONY: pkg-node
pkg-node: ## Build Node.js package
	cd packages/node && npm install && npm run build && npm run build-ext

# ── Fuzz ───────────────────────────────────────────────────────────

# cargo-fuzz needs nightly rustc. The fuzz crate lives outside the
# main workspace (its own `[workspace]` block) because libfuzzer's
# -Zsanitizer flags conflict with the stable workspace.

.PHONY: fuzz-build
fuzz-build: ## Build both fuzz targets (nightly)
	cd ../fuzz && cargo +nightly fuzz build

.PHONY: fuzz-smoke
fuzz-smoke: ## Quick 10s run of each fuzz target (crash + regression signal)
	cd ../fuzz && cargo +nightly fuzz run manifest_parse -- -max_total_time=10
	cd ../fuzz && cargo +nightly fuzz run vfs_operations -- -max_total_time=10

# ── Cleanup ────────────────────────────────────────────────────────

.PHONY: clean
clean: ## Clean build artifacts
	cargo clean
	rm -f turbolite.h

# ── Help ───────────────────────────────────────────────────────────

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

.DEFAULT_GOAL := help
