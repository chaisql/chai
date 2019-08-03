NAME := genji

.PHONY: all $(NAME) test testrace build gen

all: $(NAME)

build: $(NAME)

$(NAME):
	go install ./cmd/$@

gen: $(NAME)
	go generate ./...

test: gen
	go test -v -cover -timeout=1m ./...

testrace: gen
	go test -v -race -cover -timeout=2m ./...

bench:
	go test -v -run=^\$$ -benchmem -bench=. ./...
