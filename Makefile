PKG ?= ./...

test:
	go test -v $(PKG)
coverage:
	go test -coverprofile=.coverage/coverage.out $(PKG)
	go tool cover -html=.coverage/coverage.out -o .coverage/coverage.html
rm.tmp:
	rm -rf .tmp

PHONY: test coverage
