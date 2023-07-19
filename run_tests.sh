#!/bin/bash

full_path="$(pwd)"
#parent_path="$(dirname $full_path)"
parent_path="$full_path"

python3.9 -m unittest discover -s "$parent_path"/tests -v

