#!/bin/bash
set -e

# 取脚本所在目录的上一级（即 shell/ 目录）
home=$(cd "$(dirname "$0")/.." && pwd)

# 加载公共变量（JAVA_HOME / JAR_PATH / INPUT_PATH / OUTPUT_PATH）
source "${home}/bin/common.sh"

# 确保输出目录存在
mkdir -p /home/hadoop/data/flume/logs

echo "start analog data ****************"
java -cp /home/hadoop/shell/lib/learningspark.jar \
  com.djt.AnalogData \
  /home/hadoop/shell/data/sogoulogs.log \
  /home/hadoop/data/flume/logs/sogou.log
