#! /bin/bash
for i in "$1"/*
do
    base_name=$(basename "${i}")
    if test -f "$i"
    then
       jq --sort-keys . "$i" > "tmp/pretty-$1-$base_name"
    fi
done
