#!/bin/bash

source /home/psutra/jessy/configuration.sh
export CLASSPATH=${classpath}
java -ea -Xms1000m -Xmx2000m -XX:+UseConcMarkSweepGC com.yahoo.ycsb.CommandLine -db com.yahoo.ycsb.JessyDBClient 


