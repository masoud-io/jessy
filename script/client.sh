#!/bin/bash

source /home/pcincilla/workspaces/jessyGit/jessy/script/configuration.sh
hostname=`hostname`;
stout=${hostname}".stout"
sterr=${hostname}".sterr"

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

if [[ ${system} == "cassandra"  ]];
then
    let e=${#servers[@]}-1
    hostparam=${servers[0]}
    for i in `seq 1 $e`
    do
	hostparam=${hostparam},${servers[$i]}
    done;
    echo "hosts=${hostparam}" >>  ${workingdir}/workload
fi;

# if [[ ${workloadType} == "-load"  ]];
# then

#     records=`cat ${workingdir}/workload | grep recordcount | gawk -F'=' '{print $2}'`;
    
#     let e=${#nodes[@]};
#     share=`echo "${records} / ${e}" | bc`;
#     for i in `seq 0 ${e}`
#     do
# 	node=${nodes[$i]}
# 	if [[ ${node} == ${hostname} ]];
# 	then
# 	    start=`echo "${i} * ${share}" | bc`;
# 	    echo "insertstart=${start}" >> ${workingdir}/workload
# 	    echo "insertcount=${share}" >> ${workingdir}/workload
# 	fi;

#     done;

# fi;

cd ${workingdir};

export CLASSPATH=${classpath}
java  -Xms1000m -Xmx2000m -XX:+UseConcMarkSweepGC com.yahoo.ycsb.Client ${workloadType} -db ${clientclass} -s -threads ${nthreads} -P ${workingdir}/workload 1> ${scriptdir}/$stout 2>${scriptdir}/$sterr

cat ${scriptdir}/$stout >  ${scriptdir}/${hostname}
echo "" >>  ${scriptdir}/${hostname}
cat ${scriptdir}/$sterr >>  ${scriptdir}/${hostname}
cd ${scriptdir};
