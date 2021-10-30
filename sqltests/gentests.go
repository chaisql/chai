package generated

//go:generate rm -fr generated_tests
//go:generate go run ../dev/gensqltest -gen-dir -output-dir=generated_tests -package=generated_test -exclude=expr/* **/*.sql
