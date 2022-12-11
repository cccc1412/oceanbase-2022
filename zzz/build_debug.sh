cd ..
./build.sh debug --init --make -j9
cd build_debug
make install DESTDIR=.