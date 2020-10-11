# Fuzz Tests

Fuzzing Genji with [go-fuzz](https://github.com/dvyukov/go-fuzz).

## Quick Start

1. Install [go-fuzz](https://github.com/dvyukov/go-fuzz)
   ```
   go install github.com/dvyukov/go-fuzz/go-fuzz-build github.com/dvyukov/go-fuzz/go-fuzz
   ```

2. Download the initial corpus
   ```
   git clone https://github.com/genjidb/go-fuzz-corpus testdata/fuzz
   ```

3. Build the test program with necessary instrumentation
   ```
   go-fuzz-build -func FuzzParseQuery
   ```
   This will produce fuzz-fuzz.zip archive.

4. Run the fuzzer.
   ```
   go-fuzz -workdir=testdata/fuzz/FuzzParseQuery
   ```
   The results will be written to `testdata/fuzz/FuzzParseQuery`.
   Note that go-fuzz runs forever until manually stopped.

   Example output:
   ```
   …
   2020/10/14 12:03:58 workers: 2, corpus: 166 (13s ago), crashers: 2, restarts: 1/9663, execs: 1526845 (16418/sec), cover: 924, uptime: 1m33s
   …
   ```
   Here, `corpus` is number of interesting inputs the fuzzer has discovered, and `crashers` is number of discovered bugs (check out `testdata/fuzz/FuzzParseQuery/crashers` dir).

See [README file from go-fuzz](https://github.com/dvyukov/go-fuzz/blob/master/README.md) for more information.
