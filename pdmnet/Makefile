DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

build:
	docker swarm init || docker network create --scope=swarm --attachable -d overlay pdmnet
