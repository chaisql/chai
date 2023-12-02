NAME := chai

.PHONY: all build $(NAME) gen test testrace bench tidy

all: $(NAME)

build: $(NAME)

$(NAME):
	cd ./cmd/$@ && go install

gen:
	go generate ./...

test:
	go test -timeout=1m -cover ./...
	cd cmd/chai && go test -cover -timeout=1m ./...

testrace:
	go test -race -cover -timeout=1m ./...
	cd cmd/chai && go test -race -cover -timeout=1m ./...

bench:
	go test -v -run=^\$$ -benchmem -bench=. ./...
	cd cmd/chai && go test -v -run=^\$$ -benchmem -bench=. ./...

tidy:
	go mod tidy
	cd cmd/chai && go mod tidy && cd ../..
