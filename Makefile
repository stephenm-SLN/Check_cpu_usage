BINARY      := ssh_cpu_check
LINUX_BIN   := ssh_cpu_check_linux
SOURCE      := ssh_cpu_check.go
DEPLOY_HOST := Run_state
DEPLOY_PATH := /home/archy/local/python_server/gui_server/gui_cpu_usage/$(BINARY)

.PHONY: all mac linux deploy clean

## Build both mac and linux binaries
all: mac linux

## Build macOS binary (for local dev/testing)
mac:
	go build -o $(BINARY) ./$(SOURCE)

## Cross-compile Linux x86_64 binary (for the PM2 server)
linux:
	GOOS=linux GOARCH=amd64 go build -o $(LINUX_BIN) ./$(SOURCE)

## Build linux binary and copy it to the PM2 server
deploy: linux
	scp $(LINUX_BIN) archy@$(DEPLOY_HOST):$(DEPLOY_PATH)
	ssh $(DEPLOY_HOST) "chmod +x $(DEPLOY_PATH)"
	@echo "Deployed to $(DEPLOY_HOST):$(DEPLOY_PATH)"

## Remove compiled binaries
clean:
	rm -f $(BINARY) $(LINUX_BIN)
