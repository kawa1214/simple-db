test:
	go test -v ./...
coverage:
	go test -coverprofile=.coverage/coverage.out ./...
	go tool cover -html=.coverage/coverage.out -o .coverage/coverage.html
rm.tmp:
	rm -rf .tmp

PHONY: test coverage
