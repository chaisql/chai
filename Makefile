NAME := genji

.PHONY: all $(NAME) test testrace build gen

all: $(NAME)

build: $(NAME)

$(NAME):
	cd ./cmd/$@ && go install -ldflags="-X main.CLIVersion=$(TRAVIS_TAG) -X github.com/genjidb/genji.GenjiVersion=$(TRAVIS_TAG)"

gen: $(NAME)
	go generate ./...

test:
	go test -cover -timeout=1m ./...

testtinygo:
	go test -tags=tinygo -cover -timeout=1m ./...

bench:
	go test -v -run=^\$$ -benchmem -bench=. ./...
	cd cmd/genji && go test -v -run=^\$$ -benchmem -bench=. ./...
