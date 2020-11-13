build:
	go mod tidy
	go mod verify
	go build -o ./bin/example ./example/main.go
	go build -o ./bin/echo ./rpc/main/echo.go

fmt:
	go fmt ./...

ci:
	go test -race -v -test.timeout 2m -coverprofile=coverage.txt -covermode=atomic ./...
