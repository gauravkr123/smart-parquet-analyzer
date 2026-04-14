.PHONY: serve help

PORT ?= 8080

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

serve: ## Start a local server (default: port 8080)
	@echo "Parquet Explorer running at http://localhost:$(PORT)"
	@python3 -m http.server $(PORT)
