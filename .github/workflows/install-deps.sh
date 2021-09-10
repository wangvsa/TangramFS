#!/bin/bash

wget https://github.com/openucx/ucx/releases/download/v1.11.1/ucx-1.11.1.tar.gz

tar -zxf ./ucx-1.11.1.tar.gz

cd ./ucx-1.11.1 && mkdir install

./autogen.sh
./contrib/configure-release --without-cuda --without-java
make -j2
sudo make install
