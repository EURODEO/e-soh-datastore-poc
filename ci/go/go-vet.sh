#!/bin/bash

set -euo pipefail

# Go vet exits in case of issues
(cd ./datastore && go vet ./...)
# else
echo "No subtle issues found in the code. All is working as intended."
