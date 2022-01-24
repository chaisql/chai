NAME := genji

.PHONY: all $(NAME) test testrace build gen tidy

all: $(NAME)

build: $(NAME)

$(NAME):
	cd ./cmd/$@ && go install

gen:
	go generate ./...

test:
	go test -timeout=1m -cover ./...
	cd cmd/genji && go test -cover -timeout=1m ./...

testnodebug:
	go test -cover -timeout=1m ./...
	cd cmd/genji && go test -cover -timeout=1m ./...

testrace:
	go test -race -cover -timeout=1m ./...
	cd cmd/genji && go test -race -cover -timeout=1m ./...

bench:
	go test -v -run=^\$$ -benchmem -bench=. ./...
	cd cmd/genji && go test -v -run=^\$$ -benchmem -bench=. ./...

tidy:
	go mod tidy
	cd cmd/genji && go mod tidy && cd ../..
