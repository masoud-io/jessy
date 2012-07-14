#!/bin/bash

source /home/msaeida/jessy_script/configuration.sh
export CLASSPATH=${classpath}
java -ea -Xms1000m -Xmx2000m -XX:+UseConcMarkSweepGC com.yahoo.ycsb.CommandLine -db com.yahoo.ycsb.JessyDBClient 


