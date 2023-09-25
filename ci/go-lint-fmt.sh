#!/bin/bash

set -euo pipefail

files=$(gofmt -d -e -l -s .)

if [ -n "${files}" ]; then
  echo "${files}"
  exit 1
else
  echo "All files are in the proper format."
fi
