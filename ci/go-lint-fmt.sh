#!/bin/bash

set -euox pipefail

files=$(gofmt -d -e -l -s .) && [ -z "$files" ]
