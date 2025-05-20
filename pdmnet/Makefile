DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

build:
	docker swarm init --advertise-addr 127.0.0.1 || docker network create --scope=swarm --attachable -d overlay pdmnet
	
