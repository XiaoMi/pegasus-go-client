build:
	go build -o ./bin/example ./example/main.go

fmt:
	go fmt ./...

ci:
	golangci-lint run -c .golangci.yml --timeout 5m0s
