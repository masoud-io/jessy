#!/bin/bash

source /home/msaeida/jessy_script/configuration.sh

rm -f nohup.*

function stopExp(){

    let e=${#servers[@]}-1
    for i in `seq 0 $e`
    do
	echo "stopping on ${servers[$i]}"
	nohup ${SSHCMD} ${servers[$i]} "killall -SIGTERM java" 2&>1 > /dev/null &
    done

}

trap "stopExp; wait; exit 255" SIGINT SIGTERM


# 2 - Experimentation

let e=${#servers[@]}-1
for i in `seq 0 $e`
do
    echo "launching on ${servers[$i]}"
    nohup ${SSHCMD} ${servers[$i]} "${scriptdir}/jessy.sh"  > ${servers[$i]} &
done

wait

