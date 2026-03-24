# turbolite build targets
#
# Shared library (.so / .dylib) for C FFI consumers.
# Cargo.toml declares crate-type = ["lib", "cdylib"] so both rlib and
# shared library are produced on every build.

UNAME := $(shell uname -s)
ifeq ($(UNAME),Darwin)
  LIB_EXT := dylib
  LIB_PREFIX := lib
else
  LIB_EXT := so
  LIB_PREFIX := lib
endif

LIB_NAME := turbolite
LIB_FILE := $(LIB_PREFIX)$(LIB_NAME).$(LIB_EXT)
TARGET_DIR := target/release

# Features to include in the shared library build.
# Override with: make lib FEATURES="zstd,encryption,tiered"
FEATURES ?= zstd

# ── Shared library ─────────────────────────────────────────────────

.PHONY: lib
lib: ## Build shared library (.so / .dylib) linking system SQLite
	cargo build --release --lib --no-default-features --features $(FEATURES)
	@echo ""
	@echo "Built: $(TARGET_DIR)/$(LIB_FILE)"
	@ls -lh $(TARGET_DIR)/$(LIB_FILE)

.PHONY: lib-bundled
lib-bundled: ## Build shared library with bundled SQLite (self-contained)
	cargo build --release --lib --features $(FEATURES),bundled-sqlite
	@echo ""
	@echo "Built (bundled SQLite): $(TARGET_DIR)/$(LIB_FILE)"
	@ls -lh $(TARGET_DIR)/$(LIB_FILE)

# ── Loadable extension ────────────────────────────────────────────

EXT_FEATURES ?= zstd

.PHONY: ext
ext: ## Build SQLite loadable extension (.so / .dylib) for load_extension()
	cargo build --release --lib --no-default-features --features loadable-extension,$(EXT_FEATURES)
	@cp $(TARGET_DIR)/$(LIB_FILE) $(TARGET_DIR)/turbolite.$(LIB_EXT)
	@echo ""
	@echo "Built loadable extension: $(TARGET_DIR)/turbolite.$(LIB_EXT)"
	@ls -lh $(TARGET_DIR)/turbolite.$(LIB_EXT)

# ── C header ───────────────────────────────────────────────────────

.PHONY: header
header: ## Generate turbolite.h C header via cbindgen
	cbindgen --config cbindgen.toml --crate turbolite --output turbolite.h
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

# ── CLI + binaries (bundled SQLite) ────────────────────────────────

.PHONY: build
build: ## Build all binaries (CLI, benchmarks) with bundled SQLite
	cargo build --release

# ── Tests ──────────────────────────────────────────────────────────

.PHONY: test
test: ## Run all tests (Rust unit + FFI)
	cargo test --features zstd,bundled-sqlite

.PHONY: test-all
test-all: ## Run all tests including tiered/S3
	cargo test --features zstd,tiered,bundled-sqlite

.PHONY: test-ext
test-ext: ext ## Run loadable extension tests (requires Homebrew Python)
	/opt/homebrew/bin/python3 tests/test_loadable_ext.py

.PHONY: test-ffi
test-ffi: test-ffi-python test-ffi-c test-ffi-go test-ffi-node ## Run all FFI integration tests (Python, C, Go, Node)

.PHONY: test-ffi-tiered
test-ffi-tiered: ## Run FFI integration tests with tiered/S3 (needs S3 creds)
	$(MAKE) test-ffi FEATURES="zstd,tiered"

.PHONY: test-ffi-python
test-ffi-python: lib-bundled ## FFI test: Python (ctypes)
	python3 tests/test_ffi_python.py

# Build CFLAGS that mirror Cargo features for the C test.
FFI_C_DEFINES :=
ifneq (,$(findstring tiered,$(FEATURES)))
  FFI_C_DEFINES += -DTURBOLITE_TIERED
endif
ifneq (,$(findstring encryption,$(FEATURES)))
  FFI_C_DEFINES += -DTURBOLITE_ENCRYPTION
endif

.PHONY: test-ffi-c
test-ffi-c: lib-bundled header ## FFI test: C (proves turbolite.h works)
	cc -o $(TARGET_DIR)/test_ffi_c tests/test_ffi_c.c \
		$(FFI_C_DEFINES) \
		-L$(TARGET_DIR) -lturbolite \
		-Wl,-rpath,$(CURDIR)/$(TARGET_DIR)
	$(TARGET_DIR)/test_ffi_c

.PHONY: test-ffi-go
test-ffi-go: lib-bundled ## FFI test: Go (cgo)
	cd tests/test_ffi_go && \
		DYLD_LIBRARY_PATH=$(CURDIR)/$(TARGET_DIR) \
		CGO_LDFLAGS="-L$(CURDIR)/$(TARGET_DIR) -lturbolite" \
		go run .

.PHONY: test-ffi-node
test-ffi-node: lib-bundled ## FFI test: Node.js (koffi)
	cd tests/test_ffi_node && npm install --silent 2>/dev/null && node test.mjs

# ── Examples ──────────────────────────────────────────────────────

.PHONY: example-rust
example-rust: ## Run Rust example (native API, no shared lib needed)
	cd examples/rust && cargo run

.PHONY: example-python
example-python: lib-bundled ## Run Python example (FastAPI server)
	uv run examples/python/basic.py

.PHONY: example-c
example-c: lib-bundled header ## Build and run C example (sensor logger)
	cc -o $(TARGET_DIR)/example_c examples/c/basic.c \
		-L$(TARGET_DIR) -lturbolite \
		-Wl,-rpath,$(CURDIR)/$(TARGET_DIR)
	$(TARGET_DIR)/example_c

.PHONY: example-go
example-go: lib-bundled ## Run Go example (HTTP server)
	cd examples/go && \
		DYLD_LIBRARY_PATH=$(CURDIR)/$(TARGET_DIR) \
		CGO_LDFLAGS="-L$(CURDIR)/$(TARGET_DIR) -lturbolite" \
		go run .

.PHONY: example-node
example-node: lib-bundled ## Run Node.js example (HTTP server)
	cd examples/node && npm install --silent 2>/dev/null && node basic.mjs

# ── Packages ─────────────────────────────────────────────────────

.PHONY: pkg-python
pkg-python: ext ## Build Python package (builds ext, bundles binary, creates wheel)
	cp $(TARGET_DIR)/turbolite.$(LIB_EXT) packages/python/turbolite/
	cd packages/python && pip wheel . --no-deps -w dist/

.PHONY: pkg-python-dev
pkg-python-dev: ext ## Install Python package in dev mode (builds ext first)
	cp $(TARGET_DIR)/turbolite.$(LIB_EXT) packages/python/turbolite/
	cd packages/python && pip install -e .

.PHONY: pkg-node
pkg-node: ## Build Node.js package (napi-rs native addon)
	cd packages/node && npm install && npx @napi-rs/cli build --release --platform

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
