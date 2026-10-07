MODULES := . providers/mattn providers/libsql providers/postgres examples

.PHONY: all test test-short test-libsql test-postgres lint fmt tidy build vet check

all: check

## build: compile every module
build:
	@for m in $(MODULES); do (cd $$m && go build ./...) || exit 1; done

## vet: go vet every module
vet:
	@for m in $(MODULES); do (cd $$m && go vet ./...) || exit 1; done

## test: run every test with the race detector (mattn needs CGO_ENABLED=1)
test:
	@for m in $(MODULES); do (cd $$m && CGO_ENABLED=1 go test -race -count=1 ./...) || exit 1; done

## test-short: core module only, no cgo
test-short:
	CGO_ENABLED=0 go test -count=1 ./...

## test-libsql: libsql integration against TURBINEDB_LIBSQL_URL (e.g. local sqld)
test-libsql:
	@test -n "$(TURBINEDB_LIBSQL_URL)" || (echo "set TURBINEDB_LIBSQL_URL" && exit 1)
	cd providers/libsql && go test -count=1 -run Integration -v ./...

## test-postgres: postgres system database against TURBINEDB_POSTGRES_DSN (embedded PostgreSQL when unset)
test-postgres:
	cd providers/postgres && go test -race -count=1 ./...

## lint: golangci-lint on every module
lint:
	@for m in $(MODULES); do (cd $$m && golangci-lint run --config "$(CURDIR)/.golangci.yml" ./...) || exit 1; done

## fmt: format every module
fmt:
	@for m in $(MODULES); do (cd $$m && golangci-lint fmt --config "$(CURDIR)/.golangci.yml" ./...) || exit 1; done

## tidy: go mod tidy every module
tidy:
	@for m in $(MODULES); do (cd $$m && go mod tidy) || exit 1; done

## check: what CI runs
check: build vet lint test
