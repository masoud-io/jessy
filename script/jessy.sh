#!/bin/bash

source /home/msaeida/jessy_script/configuration.sh
hostname=`hostname`;

if [[ ${workloadType} == "-load"  ]];
then
    rm -Rf ${workingdir};
fi;

if [[ ! -e ${workingdir} ]];
then
    mkdir ${workingdir};
fi;

cd ${workingdir};
cp ${scriptdir}/config.property ${workingdir};
cp ${scriptdir}/log4j.properties ${workingdir};
cp ${scriptdir}/myfractal.xml ${workingdir};
cp ${scriptdir}/config/YCSB/workloads/${workloadName} ${workingdir}/workload
cd ${workingdir};

export CLASSPATH=${scriptdir}/commons-lang.jar:${scriptdir}/log4j.jar:${scriptdir}/jessy.jar:${scriptdir}/fractal.jar:${scriptdir}/je.jar:${scriptdir}/db.jar:${scriptdir}/concurrentlinkedhashmap.jar:${scriptdir}/netty.jar:${scriptdir}/high-scale-lib.jar;

#OLD Params for garbage collection
#-XX:+UseConcMarkSweepGC 

#BerkeleyDB Core version
#-Djava.library.path=${scriptdir}/berkeleydb_core/lib

#VirtualVM Profiling
#java -server -Dcom.sun.management.jmxremote.port=4256 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Xms2000m -Xmx2000m -XX:+UseParallelGC -XX:+AggressiveOpts -XX:+UseFastAccessorMethods fr.inria.jessy.DistributedJessy

#Yourkit Profiling
#java -agentpath:/home/msaeida/yourkit_12.0.2/bin/linux-x86-64/libyjpagent.so -server -Xms1000m -Xmx1500m -XX:+UseParallelGC -XX:+AggressiveOpts -XX:+UseFastAccessorMethods fr.inria.jessy.DistributedJessy

#hprof generator
#java -server -Xms1000m -Xmx1900m -XX:+UseParallelGC -XX:+AggressiveOpts -XX:+UseFastAccessorMethods -XX:+HeapDumpOnOutOfMemoryError fr.inria.jessy.DistributedJessy

#java -server -Xms2000m -Xmx2000m -XX:+UseConcMarkSweepGC fr.inria.jessy.DistributedJessy

java -server -Xms2000m -Xmx2000m -XX:+UseParallelGC -XX:+AggressiveOpts -XX:+UseFastAccessorMethods fr.inria.jessy.DistributedJessy

cd ${scriptdir};

