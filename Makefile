DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

build: buildjupytercli buildvmaccess

buildvmaccess:
	docker buildx build --output type=docker --tag pdmvmaccess vmaccess/

buildjupytercli:
	docker buildx build --progress=plain --output type=docker --tag jupytercli $(DIR)/jupytercli

buildsocketstreamserver:
	docker buildx build --progress=plain --output type=docker --tag socketstreamserver $(DIR)/socketstreamserver

buildkafkafakestream:
	docker buildx build --progress=plain --output type=docker --tag kafkafakestream $(DIR)/kafkafakestream
