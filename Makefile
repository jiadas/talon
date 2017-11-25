#!/usr/bin/env bash

all: dev run

fmt:
	goimports -l -w  ./

usevendor:
	govendor add +e
	govendor remove +u

install: fmt clean usevendor

clean:
	rm -rf output/var/

gotest:
	 go test ./... | grep -v "^ok "

dev: install
	go build -o output/bin/talon ./app/

run:
	output/bin/talon -environment=dev
