.PHONY: help list-benchmarks bench-list bench bench-debug bench-all check clippy test

BENCH ?=
PROFILE ?= release
ARGS ?=

# Default target
help:
	@echo "rdmalib - Benchmarks & Development Makefile"
	@echo ""
	@echo "Benchmark targets:"
	@echo "  make list-benchmarks              List all configured cluster benchmarks"
	@echo "  make bench BENCH=<name>           Run a benchmark in release mode (default)"
	@echo "                                    Example: make bench BENCH=read_latency ARGS=\"--iters 50000 --size 1024\""
	@echo "  make bench-debug BENCH=<name>     Run a benchmark in debug mode"
	@echo "  make bench-all                    Run all configured benchmarks sequentially"
	@echo ""
	@echo "Development targets:"
	@echo "  make check                        Run cargo check across the workspace"
	@echo "  make clippy                       Run cargo clippy across the workspace"
	@echo "  make test                         Run library unit tests inside the container"
	@echo ""

list-benchmarks:
	@./podman_build/run_bench.sh --list

bench-list: list-benchmarks

bench:
ifeq ($(strip $(BENCH)),)
	@echo "Error: BENCH is not specified."
	@echo "Usage: make bench BENCH=<benchmark_name> [PROFILE=release|debug] [ARGS=\"--arg value\"]"
	@echo "Available benchmarks:"
	@./podman_build/run_bench.sh --list
	@exit 1
else
	@if [ "$(PROFILE)" = "debug" ]; then \
		./podman_build/run_bench.sh --debug $(BENCH) $(if $(strip $(ARGS)),-- $(ARGS),); \
	else \
		./podman_build/run_bench.sh --release $(BENCH) $(if $(strip $(ARGS)),-- $(ARGS),); \
	fi
endif

bench-debug:
ifeq ($(strip $(BENCH)),)
	@echo "Error: BENCH is not specified."
	@echo "Usage: make bench-debug BENCH=<benchmark_name> [ARGS=\"--arg value\"]"
	@exit 1
else
	@$(MAKE) bench BENCH=$(BENCH) PROFILE=debug ARGS="$(ARGS)"
endif

bench-all:
	@if [ "$(PROFILE)" = "debug" ]; then \
		./podman_build/run_bench.sh --debug --all $(if $(strip $(ARGS)),-- $(ARGS),); \
	else \
		./podman_build/run_bench.sh --release --all $(if $(strip $(ARGS)),-- $(ARGS),); \
	fi

check:
	cargo check --workspace

clippy:
	cargo clippy --workspace --all-targets

test:
	./podman_build/link_test.sh --lib
