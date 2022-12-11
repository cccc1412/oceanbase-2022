cd ..
./build.sh release --init --make -j9
cd build_release
make install DESTDIR=.