##############################
# ENV
#
SHELL := /usr/bin/env bash

.PHONY: pre-check
pre-check:
	@command -v virtualenv >/dev/null 2>&1 || { echo >&2 "$$DEPS_TEXT Missing package: virtualenv"; exit 1; }
	@command -v curl-config --version >/dev/null 2>&1 || { echo >&2 "$$DEPS_TEXT Missing package: libcurl"; exit 1; }
	@echo "int main(){}" | gcc -o /dev/null -x c - -lncurses 2>/dev/null || { echo >&2 "$$DEPS_TEXT Missing package: libncurses"; exit 1; }

.PHONY: deps
deps: pre-check venv/bin/activate .git/hooks/pre-commit
	@source venv/bin/activate && ./scripts/apsw_install.sh

.PHONY: venv
venv: venv/bin/activate
venv/bin/activate: requirements.txt
	test -d venv || virtualenv venv
	. venv/bin/activate; easy_install readline
	. venv/bin/activate; pip install -r requirements.txt
	touch venv/bin/activate

.git/hooks/pre-commit: .pre-commit
	@cp .pre-commit .git/hooks/pre-commit
	@chmod +x .git/hooks/pre-commit

##############################
# HELPERS
#

.PHONY: todo
todo:
	@ag "TODO" --ignore Makefile

.PHONY: flake8
flake8:
	source venv/bin/activate; flake8 --config=.flake8 .

.PHONY: console
console:
	source venv/bin/activate; ipython -i scripts/ipython.py

.PHONY: cloc
cloc:
	cloc --by-file-by-lang bin memsql_loader setup.py

##############################
# BUILD
#
MEMSQL_LOADER_VERSION := $(shell python -c "import memsql_loader; print memsql_loader.__version__")
export MEMSQL_LOADER_VERSION

.PHONY: version
version:
	@echo $(MEMSQL_LOADER_VERSION)

.PHONY: clean
clean:
	-make -C distribution clean
	rm -f logdict2.7.4.final*
	rm -rf *.egg memsql_loader.egg-info dist build
	python setup.py clean --all
	for _kill_path in $$(find . -type f -name "*.pyc"); do rm -f $$_kill_path; done
	for _kill_path in $$(find . -name "__pycache__"); do rm -rf $$_kill_path; done

distribution/dist/memsql-loader.tar.gz: distribution/memsql_loader.spec
	make -C distribution build

.PHONY: build
build: clean distribution/dist/memsql-loader.tar.gz

.PHONY: release
release: distribution/dist/memsql-loader.tar.gz
	git tag -f "$(MEMSQL_LOADER_VERSION)" && git push --tags -f
	@sleep 1
	-github-release info -u memsql -r memsql-loader
	-github-release delete -u memsql -r memsql-loader \
		--tag "$(MEMSQL_LOADER_VERSION)"
	github-release release -u memsql -r memsql-loader \
		--tag "$(MEMSQL_LOADER_VERSION)" \
		--name "MemSQL Loader $(MEMSQL_LOADER_VERSION)" \
		--description "$$(./scripts/latest_changes.py)" \
		--draft
	github-release upload -u memsql -r memsql-loader \
		--tag "$(MEMSQL_LOADER_VERSION)" \
		--name "memsql-loader.tar.gz" \
		--file "distribution/dist/memsql-loader.tar.gz"
	@echo "The release has been uploaded as a draft. View/Edit/Delete it here:"
	@echo "https://github.com/memsql/memsql-loader/releases"
