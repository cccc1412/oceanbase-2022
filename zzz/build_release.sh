cd ..
./build.sh release --init --make -j24
cd build_release
make install DESTDIR=.