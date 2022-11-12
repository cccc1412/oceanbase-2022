# 首先需要启动集群
cd ../tools/deploy
obd test mysqltest final_2022 --test-dir ./mysql_test/test_suite/expr/t --result-dir ./mysql_test/test_suite/expr/r --test-set expr_ceil \
# --user root --password "" --database oceanbase