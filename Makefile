.PHONY: build build-linux build-macos ci clean db help release

# Binary name
BINARY_NAME := harvester

# Build directories
BUILD_DIR := target/release
DIST_DIR := dist

# Target platforms
LINUX_TARGET := x86_64-unknown-linux-gnu
MACOS_ARM_TARGET := aarch64-apple-darwin

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Available targets:'
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-20s %s\n", $$1, $$2}'

build: ## Build for current platform (release mode)
	cargo build --release

build-linux: ## Build for Linux x86_64
	cargo build --release --target $(LINUX_TARGET)
	@mkdir -p $(DIST_DIR)
	@cp target/$(LINUX_TARGET)/release/$(BINARY_NAME) $(DIST_DIR)/$(BINARY_NAME)-linux-x86_64
	@echo "Built: $(DIST_DIR)/$(BINARY_NAME)-linux-x86_64"

build-macos: ## Build for macOS ARM64 (requires macOS or cross-compilation setup)
	@echo "Building for macOS ARM64..."
	cargo build --release --target $(MACOS_ARM_TARGET)
	@mkdir -p $(DIST_DIR)
	@cp target/$(MACOS_ARM_TARGET)/release/$(BINARY_NAME) $(DIST_DIR)/$(BINARY_NAME)-macos-arm64
	@echo "Built: $(DIST_DIR)/$(BINARY_NAME)-macos-arm64"

build-all: build-linux build-macos ## Build for all supported platforms

ci: test check fmt lint ## Run all checks

clean: ## Clean build artifacts
	cargo clean
	rm -rf $(DIST_DIR)

check: ## Run cargo check
	cargo check

db: ## Create the sqlx database query files
	cargo sqlx prepare

fmt: ## Format code
	cargo fmt

lint: ## Run clippy
	cargo clippy --all-targets -- -D warnings

install: build ## Install binary to ~/.cargo/bin
	cargo install --path .

release: clean build-all ## Create release builds for distribution
	@echo "Release builds created in $(DIST_DIR)/"
	@ls -lh $(DIST_DIR)/

test: ## Run tests
	cargo test
