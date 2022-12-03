cd ..
./build.sh debug --init --make -j12
cd build_debug
make install DESTDIR=.