#!/bin/bash

wget https://github.com/openucx/ucx/releases/download/v1.11.1/ucx-1.11.1.tar.gz

tar -zxf ./ucx-1.11.1.tar.gz

cd ./ucx-1.11.1 && mkdir install

export UCX_DIR=`pwd`/install
echo $UCX_DIR

./autogen.sh
./contrib/configure-release --prefix=$UCX_DIR --without-cuda --without-java
make -j2
make install
