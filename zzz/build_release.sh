cd ..
./build.sh release --init --make -j12
cd build_release
make install DESTDIR=.