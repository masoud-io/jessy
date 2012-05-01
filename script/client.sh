#!/bin/bash

source  /home/msaeida/jessy_script/configuration.sh
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
cp ${scriptdir}/${workloadName} ${workingdir};

if [[ ${workloadType} == "-load"  ]];
then

    records=`cat ${workingdir}/${workloadName} | grep recordcount | gawk -F'=' '{print $2}'`;
    
    let e=${#nodes[@]};
    share=`echo "${records} / ${e}" | bc`;
    for i in `seq 0 ${e}`
    do
	node=${nodes[$i]}
	if [[ ${node} == ${hostname} ]];
	then
	    start=`echo "${i} * ${share}" | bc`;
	    echo "insertstart=${start}" >> ${workingdir}/workload
	    echo "insertcount=${share}" >> ${workingdir}/workload
	fi;

    done;

fi;

cd ${workingdir};

export CLASSPATH=${scriptdir}/commons-lang.jar:${scriptdir}/log4j.jar:${scriptdir}/jessy.jar:${scriptdir}/fractal.jar:${scriptdir}/je.jar:${scriptdir}/db.jar;

java -ea -Xms1000m -Xmx2000m -XX:+UseConcMarkSweepGC com.yahoo.ycsb.Client ${workloadType} -db com.yahoo.ycsb.JessyDBClient -s -threads ${nthreads} -P ${workingdir}/${workloadName}  &> ${scriptdir}/${hostname}

cd ${scriptdir};

