CURRENT_DIR=/home/justme/oceanbase-2022
TARGET_FILE=compile_commands.json
./build.sh debug --init -DCMAKE_EXPORT_COMPILE_COMMANDS=ON
mv build_debug/ 

