DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

build: pdmnet-local

pdmnet-local:
	docker network rm pdmnet-local 2>/dev/null || true
	docker network create pdmnet-local

pdmnet-swarm:
	docker network rm pdmnet-swarm 2>/dev/null || true
	docker swarm init || true
	docker network create --scope=swarm --attachable -d overlay pdmnet-swarm
