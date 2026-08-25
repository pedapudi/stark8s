.PHONY: generate build test image local-up local-down

generate:
	go run sigs.k8s.io/controller-tools/cmd/controller-gen object paths=./api/...
	go run sigs.k8s.io/controller-tools/cmd/controller-gen crd paths=./api/... output:crd:dir=config/crd

build:
	go build ./...

test:
	go test ./...

image:
	docker build -t stark8s:dev .

local-up:
	hack/local-up.sh

local-down:
	hack/local-down.sh
