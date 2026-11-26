# Thin aliases over scripts/ (the scripts also run directly from Git Bash on Windows).
.DEFAULT_GOAL := help
.PHONY: help test

help: ## List targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-18s %s\n", $$1, $$2}'

test: ## Lint + unit tests in the test container
	./scripts/test.sh
