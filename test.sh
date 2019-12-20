#!/usr/bin/env bash

mkdir -p results/

for test_file in test_*.py; do
  pytest "$test_file" | tee results/"$(basename "$test_file" .py).out"
done
