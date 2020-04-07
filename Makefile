NAME := genji

.PHONY: all $(NAME) test testrace build gen

all: $(NAME)

build: $(NAME)

$(NAME):
	cd ./cmd/$@ && go install

gen: $(NAME)
	go generate ./...

test:
	go test -v -cover -timeout=1m ./...

testtinygo:
	go test -tags=tinygo -v -cover -timeout=1m ./...


bench:
	go test -v -run=^\$$ -benchmem -bench=. ./...
	cd cmd/genji && go test -v -run=^\$$ -benchmem -bench=. ./...
