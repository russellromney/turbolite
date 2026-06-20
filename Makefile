# turbolite build targets
#
# The cdylib + C FFI + loadable-extension surface now lives in the sibling
# `turbolite-ffi` crate. This Makefile only builds turbolite's pure-Rust
# library / bins / examples. For .dylib/.so / turbolite.h, run
# `make -C ../turbolite-ffi {lib,ext,header}`.

UNAME := $(shell uname -s)
ifeq ($(UNAME),Darwin)
  LIB_EXT := dylib
  LIB_PREFIX := lib
else
  LIB_EXT := so
  LIB_PREFIX := lib
endif

# The turbolite-ffi cdylib is `libturbolite_ffi.<ext>`; make ext also
# copies it to `turbolite.<ext>` for SQLite's `.load turbolite` path.
FFI_LIB_NAME := turbolite_ffi
FFI_LIB_FILE := $(LIB_PREFIX)$(FFI_LIB_NAME).$(LIB_EXT)
CARGO_TARGET_DIR ?= ../cinch-target
TARGET_DIR := $(CARGO_TARGET_DIR)/release
# turbolite-ffi is a workspace member (nested subdirectory), not a sibling.
FFI_DIR := turbolite-ffi

# Features forwarded to cargo.
FEATURES ?= zstd

QUINT ?= npx -y @informalsystems/quint@0.32.0
OPENJDK_PREFIX ?= $(shell brew --prefix openjdk 2>/dev/null)
JAVA_PATH_PREFIX := $(if $(OPENJDK_PREFIX),$(OPENJDK_PREFIX)/bin:)
SPEC_NEGATIVE_STEPS := \
	badWrongEpochStep \
	badWrongWriterStep \
	badSkippedSeqStep \
	badPrevChecksumStep \
	badEquivocationStep \
	badChecksumCollisionStep \
	badStaleWriterAfterPromotionStep \
	badPromotionCursorStep \
	badPromotionPageCountStep
SPEC_PREFETCH_NEGATIVE_STEPS := \
	badClaimBeforePermit \
	badWorkerBlocksBeforePermit \
	badRangeAndFullDuplicate \
	badTreeNameGatedScanRange
SPEC_MANIFEST_CAS_NEGATIVE_STEPS := \
	badSkipCasOnFirstWrite \
	badBlindOverwrite

# ── FFI build passthrough ─────────────────────────────────────────

.PHONY: lib lib-bundled ext ext-local header
lib lib-bundled: ## Build standalone cdylib (delegates to ../turbolite-ffi)
	$(MAKE) -C $(FFI_DIR) lib FEATURES="$(FEATURES)"

ext: ## Build loadable extension (delegates to ../turbolite-ffi)
	$(MAKE) -C $(FFI_DIR) ext

ext-local: ## Build loadable extension, local-only (delegates)
	$(MAKE) -C $(FFI_DIR) ext-local

header: ## Generate turbolite.h (delegates)
	$(MAKE) -C $(FFI_DIR) header

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

.PHONY: specs-typecheck
specs-typecheck: ## Typecheck Quint substrate specs
	$(QUINT) typecheck specs/cursor_chain.qnt
	$(QUINT) typecheck specs/cursor_chain_liveness.qnt
	$(QUINT) typecheck specs/cloud_scan_prefetch.qnt
	$(QUINT) typecheck specs/manifest_cas.qnt

.PHONY: specs
specs: specs-typecheck ## Run Quint substrate specs (typecheck + simulator invariants)
	$(QUINT) run specs/cursor_chain.qnt \
		--max-samples=200 --max-steps=8 --invariants=safety
	$(QUINT) run specs/cloud_scan_prefetch.qnt \
		--max-samples=500 --max-steps=8 --invariants=safety --verbosity=0
	$(QUINT) run specs/manifest_cas.qnt \
		--max-samples=500 --max-steps=8 --invariants=safety --verbosity=0

.PHONY: specs-progress
specs-progress: specs-typecheck ## Run deterministic Quint progress scenario
	$(QUINT) run specs/cursor_chain.qnt \
		--step=progressStep --max-samples=1 --max-steps=4 \
		--invariants=safety boundedProgress

.PHONY: specs-negative
specs-negative: specs-typecheck ## Run Quint specs that are expected to violate safety
	@for step in $(SPEC_NEGATIVE_STEPS); do \
		out=$$(mktemp "$${TMPDIR:-/tmp}/turbolite-quint-$$step.XXXXXX") || exit 1; \
		echo "expecting safety violation: $$step"; \
		if $(QUINT) run specs/cursor_chain.qnt \
			--max-samples=20 --max-steps=2 --step=$$step \
			--invariants=safety --verbosity=0 \
			>"$$out" 2>&1; then \
			cat "$$out"; \
			rm -f "$$out"; \
			echo "expected $$step to violate safety"; \
			exit 1; \
		elif ! grep -q "Invariant violated" "$$out"; then \
			cat "$$out"; \
			rm -f "$$out"; \
			echo "expected $$step to fail by violating safety"; \
			exit 1; \
		fi; \
		rm -f "$$out"; \
	done
	@for step in $(SPEC_PREFETCH_NEGATIVE_STEPS); do \
		out=$$(mktemp "$${TMPDIR:-/tmp}/turbolite-quint-$$step.XXXXXX") || exit 1; \
		echo "expecting safety violation: $$step"; \
		if $(QUINT) run specs/cloud_scan_prefetch.qnt \
			--max-samples=20 --max-steps=1 --step=$$step \
			--invariants=safety --verbosity=0 \
			>"$$out" 2>&1; then \
			cat "$$out"; \
			rm -f "$$out"; \
			echo "expected $$step to violate safety"; \
			exit 1; \
		elif ! grep -q "Invariant violated" "$$out"; then \
			cat "$$out"; \
			rm -f "$$out"; \
			echo "expected $$step to fail by violating safety"; \
			exit 1; \
		fi; \
		rm -f "$$out"; \
	done
	@for step in $(SPEC_MANIFEST_CAS_NEGATIVE_STEPS); do \
		out=$$(mktemp "$${TMPDIR:-/tmp}/turbolite-quint-$$step.XXXXXX") || exit 1; \
		echo "expecting safety violation: $$step"; \
		if $(QUINT) run specs/manifest_cas.qnt \
			--max-samples=20 --max-steps=2 --step=$$step \
			--invariants=safety --verbosity=0 \
			>"$$out" 2>&1; then \
			cat "$$out"; \
			rm -f "$$out"; \
			echo "expected $$step to violate safety"; \
			exit 1; \
		elif ! grep -q "Invariant violated" "$$out"; then \
			cat "$$out"; \
			rm -f "$$out"; \
			echo "expected $$step to fail by violating safety"; \
			exit 1; \
		fi; \
		rm -f "$$out"; \
	done

.PHONY: specs-all
specs-all: specs specs-progress specs-negative specs-rust specs-verify specs-liveness specs-liveness-negative ## Run all local spec checks

.PHONY: specs-verify
specs-verify: specs-typecheck ## Run Quint bounded model checker (requires Java)
	PATH="$(JAVA_PATH_PREFIX)$$PATH" $(QUINT) verify specs/cursor_chain.qnt \
		--max-steps=8 --invariants=safety
	PATH="$(JAVA_PATH_PREFIX)$$PATH" $(QUINT) verify specs/cursor_chain.qnt \
		--step=progressStep --max-steps=4 --invariants=safety boundedProgress
	PATH="$(JAVA_PATH_PREFIX)$$PATH" $(QUINT) verify specs/manifest_cas.qnt \
		--max-steps=6 --invariants=safety

.PHONY: specs-rust
specs-rust: ## Run Rust tests that bridge the Quint substrate contract
	cargo test --features zstd,bundled-sqlite replay_cursor_contract

.PHONY: specs-liveness
specs-liveness: specs-typecheck ## Run TLC temporal liveness check under explicit fairness
	PATH="$(JAVA_PATH_PREFIX)$$PATH" $(QUINT) verify specs/cursor_chain_liveness.qnt \
		--backend=tlc --max-steps=8 --invariants=safety \
		--temporal=fairStablePrefixProgress

.PHONY: specs-liveness-negative
specs-liveness-negative: specs-typecheck ## Run temporal property expected to fail without fairness
	@out=$$(mktemp "$${TMPDIR:-/tmp}/turbolite-quint-badUnfairPollingProgress.XXXXXX") || exit 1; \
	if PATH="$(JAVA_PATH_PREFIX)$$PATH" $(QUINT) verify specs/cursor_chain_liveness.qnt \
		--backend=tlc --max-steps=8 --invariants=safety \
		--temporal=badUnfairPollingProgress \
		>"$$out" 2>&1; then \
		cat "$$out"; \
		rm -f "$$out"; \
		echo "expected badUnfairPollingProgress to violate temporal progress"; \
		exit 1; \
	elif ! grep -q "Temporal properties were violated" "$$out"; then \
		cat "$$out"; \
		rm -f "$$out"; \
		echo "expected badUnfairPollingProgress to fail by violating temporal progress"; \
		exit 1; \
	fi; \
	rm -f "$$out"

.PHONY: test-ext test-ffi test-ffi-tiered test-ffi-python test-ffi-c test-ffi-go test-ffi-node
test-ext test-ffi test-ffi-tiered test-ffi-python test-ffi-c test-ffi-go test-ffi-node: ## FFI / loadable-ext tests live in turbolite-ffi now
	$(MAKE) -C $(FFI_DIR) $@

# ── Examples ──────────────────────────────────────────────────────

.PHONY: example-rust
example-rust: ## Run Rust local example (native API, no shared lib needed)
	cd examples/rust && cargo run --bin local

.PHONY: example-rust-tiered
example-rust-tiered: ## Run Rust S3 tiered example
	cd examples/rust && cargo run --bin tiered

.PHONY: example-python
example-python: lib-bundled ## Run Python local example (FastAPI server)
	uv run examples/python/local.py

.PHONY: example-python-tiered
example-python-tiered: lib-bundled ## Run Python S3 tiered example
	uv run examples/python/tiered.py

.PHONY: example-c
example-c: lib-bundled header ## Build and run C local example (sensor logger)
	cc -o $(TARGET_DIR)/example_c examples/c/local.c \
		-L$(TARGET_DIR) -lturbolite_ffi \
		-Wl,-rpath,$(CURDIR)/$(TARGET_DIR)
	$(TARGET_DIR)/example_c

.PHONY: example-c-tiered
example-c-tiered: lib-bundled header ## Build and run C S3 tiered example
	cc -o $(TARGET_DIR)/example_c_tiered examples/c/tiered.c \
		-L$(TARGET_DIR) -lturbolite_ffi \
		-Wl,-rpath,$(CURDIR)/$(TARGET_DIR)
	$(TARGET_DIR)/example_c_tiered

.PHONY: example-go
example-go: lib-bundled ## Run Go local example (HTTP server)
	cd examples/go && \
		DYLD_LIBRARY_PATH=$(CURDIR)/$(TARGET_DIR) \
		CGO_LDFLAGS="-L$(CURDIR)/$(TARGET_DIR) -lturbolite_ffi" \
		go run local.go

.PHONY: example-go-tiered
example-go-tiered: lib-bundled ## Run Go S3 tiered example (HTTP server)
	cd examples/go && \
		DYLD_LIBRARY_PATH=$(CURDIR)/$(TARGET_DIR) \
		CGO_LDFLAGS="-L$(CURDIR)/$(TARGET_DIR) -lturbolite_ffi" \
		go run tiered.go

.PHONY: example-node
example-node: lib-bundled ## Run Node.js local example (HTTP server)
	cd examples/node && npm install --silent 2>/dev/null && node local.mjs

.PHONY: example-node-tiered
example-node-tiered: lib-bundled ## Run Node.js S3 tiered example (HTTP server)
	cd examples/node && npm install --silent 2>/dev/null && node tiered.mjs

# ── Packages ─────────────────────────────────────────────────────

.PHONY: pkg-python pkg-python-dev pkg-node
pkg-python pkg-python-dev pkg-node: ## Packages live in ../turbolite-ffi now
	@echo "Language packages moved to ../turbolite-ffi/packages as of Phase Cirrus h3."
	@echo "Run: make -C ../turbolite-ffi $@"
	@exit 1

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
