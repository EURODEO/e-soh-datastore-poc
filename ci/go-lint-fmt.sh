#!/bin/bash

set -euo pipefail

files=$(gofmt -d -e -l -s .)

if [ -n "${files}" ]; then
  echo "${files}"
  exit 1
fi
