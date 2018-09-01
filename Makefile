NAME := genji

.PHONY: all $(NAME) test testrace build gen

all: $(NAME)

build: $(NAME)

$(NAME):
	go install ./cmd/$@

gen:
	go generate ./...

test: gen
	go test -v -cover -timeout=1m ./...

testrace: gen
	go test -v -race -cover -timeout=2m ./...
