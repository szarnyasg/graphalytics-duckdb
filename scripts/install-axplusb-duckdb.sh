#!/bin/bash

set -eu
set -o pipefail

cd "$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd ../scratch

# ensure that the Ninja build system is installed
if [[ ! -z $(which yum) ]]; then
    sudo yum install -y ninja-build
elif [[ ! -z $(which apt-get) ]]; then
    sudo apt-get update
    sudo apt-get install -y ninja-build
elif [ "$(uname)" == "Darwin" ]; then
    brew install ninja
else
    echo "Operating system not supported, please install the dependencies manually"
fi

# build Python package
rm -rf duckdb
git clone https://github.com/szarnyasg/duckdb
cd duckdb
git checkout axplusb
GEN=ninja BUILD_PYTHON=1 make
