#!/bin/sh

cat ./src/lib.rs | grep -v '^$' | grep -v '^ *//' | grep -v '^ *#' | grep -v '^ *log::' | wc -l
