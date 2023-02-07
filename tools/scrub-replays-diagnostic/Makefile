SHELL=/bin/bash


clean:
	rm -rf ./output/*
	rm -rf ./diffs/*
	rm -rf ./tmp/*
.PHONY: clean

run: clean
	./pretty-json.sh input
	cargo run -q
	./pretty-json.sh output
	./diff.sh input output


