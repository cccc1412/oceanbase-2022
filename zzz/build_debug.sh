cd ..
./build.sh debug --init --make -j24
cd build_debug
make install DESTDIR=.