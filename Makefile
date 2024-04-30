.PHONY: test fuzz

test:
	go test -v

fuzz:
	go test -fuzz FuzzSQLParser
