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
cd ${workingdir};

export CLASSPATH=${scriptdir}/commons-lang.jar:${scriptdir}/log4j.jar:${scriptdir}/jessy.jar:${scriptdir}/fractal.jar:${scriptdir}/je.jar:${scriptdir}/db.jar;

#-Dcom.sun.management.jmxremote.port=4256 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false
java -ea -Xms1000m -Xmx2000m -XX:+UseConcMarkSweepGC fr.inria.jessy.DistributedJessy 

cd ${scriptdir};

