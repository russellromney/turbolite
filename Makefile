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
CARGO_TARGET_DIR ?= ../cinch-target
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
	cp $(TARGET_DIR)/turbolite.$(LIB_EXT) $(TARGET_DIR)/turbolite.$(LIB_EXT)
	/opt/homebrew/bin/python3 tests/test_auto_extension_python.py

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
