DISTRIBUTIONDIR = $(shell pwd)
BASEDIR = $(shell dirname $(DISTRIBUTIONDIR))

.PHONY: clean
clean:
	rm -rf ./build
	rm -rf ./dist

.PHONY: check-env
check-env:
ifndef MEMSQL_LOADER_VERSION
	$(error MEMSQL_LOADER_VERSION must be defined)
else
	@echo "Building Memsql Loader $(MEMSQL_LOADER_VERSION)"
endif

.PHONY: build
build: check-env ./dist/memsql-loader
./dist/memsql-loader: ./memsql_loader.spec
	# build the base
	docker build -t "memsql/memsql-loader" .
	# gogo docker build
	docker run \
	    -v $(BASEDIR):/memsql_loader \
	    memsql/memsql-loader \
	    /memsql_loader/distribution/builder.sh $(MEMSQL_LOADER_VERSION) /memsql_loader $(shell id -u)
