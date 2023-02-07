#! /bin/bash
for i in "$2"/*
do
    base_name=$(basename "${i}")
    if test -f "$i"
    then
       diff "tmp/pretty-$1-$base_name" "tmp/pretty-$2-$base_name" > "diffs/${base_name}.diff"
    fi
done
exit 0
