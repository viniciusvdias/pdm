DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

build:
	docker buildx build --output type=docker --tag pdmvmaccess vmaccess/
	docker buildx build --progress=plain --output type=docker --tag jupytercli $(DIR)/jupytercli
