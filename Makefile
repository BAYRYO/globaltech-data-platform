# Thin aliases over scripts/ (the scripts also run directly from Git Bash on Windows).
.DEFAULT_GOAL := help
.PHONY: help test
.PHONY: cluster-up cluster-down cluster-reset cluster-status cluster-init cluster-smoke cluster-shell

help: ## List targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-18s %s\n", $$1, $$2}'

test: ## Lint + unit tests in the test container
	./scripts/test.sh

cluster-up: ## Start the HDFS/YARN cluster and wait until it is healthy
	./scripts/cluster/up.sh

cluster-down: ## Stop the cluster, keep HDFS data
	./scripts/cluster/down.sh

cluster-reset: ## Delete the cluster and all HDFS data
	./scripts/cluster/reset.sh

cluster-status: ## Live DataNodes, NodeManagers and web UIs
	./scripts/cluster/status.sh

cluster-init: ## Create the HDFS tree, ACLs, quotas and the Spark jars archive (idempotent)
	./scripts/cluster/init-hdfs.sh

cluster-smoke: ## End-to-end check: Spark, MapReduce and Hadoop Streaming on YARN
	./scripts/cluster/smoke/run.sh

cluster-shell: ## Interactive client shell on the cluster network
	./scripts/cluster/client.sh
