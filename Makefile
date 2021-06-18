tidy:
	test "${CI}" || go mod tidy

fmt:
	test "${CI}" || gofmt -s -w .
	test "${CI}" == "" || test -z "`gofmt -d . | tee /dev/stderr`"

vet:
	go vet ./...

test:
	go test -v ./...

check: tidy fmt vet test

publish: # Forces refresh of go.dev
	GOPROXY=https://proxy.golang.org go get github.com/koyeb/kreconciler
