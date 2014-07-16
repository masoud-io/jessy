#!/bin/bash

source /root/distemjessy/scripts/configuration.sh

rm -f nohup.*

function stopExp(){

    let e=${#servers[@]}-1
    for i in `seq 0 $e`
    do
	echo "stopping on ${servers[$i]}"
	#distem --execute vnode=${servers[$i]},command="ps -ef | grep java | awk '{print $2}' | xargs kill -SIGTERM" 2&>1 > /dev/null
	nohup ${SSHCMD} -o 'StrictHostKeyChecking no' ${servers[$i]} "ps -ef | grep java | awk '{print \$2}' | xargs kill -SIGTERM" 2&>1 > /dev/null &
    done

}

trap "stopExp; wait; exit 255" SIGINT SIGTERM


# 2 - Experimentation

let e=${#servers[@]}-1
for i in `seq 0 $e`
do
    echo "launching on ${servers[$i]}"
    #distem --execute vnode=${servers[$i]},command="source ${scriptdir}/jessy.sh > ${servers[$i]} &"
    nohup ${SSHCMD} -o 'StrictHostKeyChecking no' ${servers[$i]} "${scriptdir}/jessy.sh"  > ${servers[$i]} &
done

wait

