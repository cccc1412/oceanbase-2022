rm perf.data
rm out.*
passwd="201768"
echo $passwd| sudo -S perf record -F 199 -p $(pidof observer) -g sleep 480
# -F 采样频率
# -g 生成函数调用 call gragh
# -p 指定进程
sudo chown nxz:nxz perf.data
perf script > out.perf
# 读取 perf.data 二进制，生成分析报告
stackcollapse-perf.pl out.perf > out.folded
# 格式转换
flamegraph.pl out.folded > cpu.svg
# sudo grep cpuid out.folded | flamegraph.pl > cpuid.svg
exit
