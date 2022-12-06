#include <cstdint>
#include <stdio.h>

int main() {
    int8_t x=INT8_MIN;
    int8_t y=INT8_MAX;
    uint8_t z = y - x;
    printf("%d,%d,%d\n", x,y,z);
    return 0;
}