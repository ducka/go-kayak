#BIN 						:= $(PWD)/bin
#export GOBIN				:= $(BIN)
#export DOCKER_BUILDKIT		:= 1

#ifneq (,$(findstring Apple M1, $(LAPTOP_PLATFORM)))

# for M1 chips always build docker with amd for stability reasons
#DOCKER_BUILD_PLATFORM_ARG=--platform linux/amd64

#endif

# --silent drops the need to prepend `@` to suppress command output.
#MAKEFLAGS += --silent

.PHONY: help
help: ## Tell me what everything does!
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.PHONY: setup
setup:
	docker-compose -f docker-compose.yml up
	docker-compose run --rm wait wfi redis:7007 -t 10

.PHONY: clean
clean-databases:
	docker-compose down
	docker image prune -f