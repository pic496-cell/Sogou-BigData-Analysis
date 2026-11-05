#!/bin/bash
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
JAR_PATH=/home/hadoop/shell/lib/learningspark.jar
INPUT_PATH=/home/hadoop/shell/data/sogoulogs.log
OUTPUT_PATH=/home/hadoop/data/flume/logs/sogou.log
