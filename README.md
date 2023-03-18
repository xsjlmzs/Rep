# Rep
**protobuf** : v3.20.1

```shell
apt update -y && apt install -y git curl wget gcc g++ cmake pkg-config autoconf libtool
```



```shell
# protobuf
wget https://github.com/protocolbuffers/protobuf/releases/download/v3.20.1/protobuf-cpp-3.20.1.tar.gz
tar -zxf protobuf-cpp-3.20.1.tar.gz
mv protobuf-3.20.1/ protobuf
rm -rf protobuf-cpp-3.20.1.tar.gz
cd protobuf
./autogen.sh
./configure --prefix=/usr/local
make -j
make install
cd ..
```

```shell
# ZeroMQ
# Build, check, and install libsodium
git clone git://github.com/jedisct1/libsodium.git
cd libsodium
./autogen.sh 
./configure && make check 
sudo make install 
sudo ldconfig
cd ../
# Build, check, and install the latest version of ZeroMQ
git clone git://github.com/zeromq/libzmq.git
cd libzmq
./autogen.sh 
./configure --with-libsodium && make
sudo make install
sudo ldconfig
cd ../
# Now install ZMQPP
git clone git://github.com/zeromq/zmqpp.git
cd zmqpp
make
make check
sudo make install
make installcheck
```

```shell
# glog
wget https://github.com/google/glog/archive/v0.3.4.tar.gz
tar -xzf v0.3.4.tar.gz
mv glog-0.3.4/ glog
rm -fr v0.3.4.tar.gz
cd glog
./configure --prefix=$PWD
make -j
make install
cd ..
```

