#!/bin/bash

source  /home/msaeida/jessy_script/configuration.sh

rm -f nohup.*

function stopExp(){

    let e=${#clients[@]}-1
    for i in `seq 0 $e`
    do
	echo "stopping on ${clients[$i]}"
	nohup ${SSHCMD} ${clients[$i]} "killall -SIGTERM java" 2&>1 > /dev/null &
    done

}

trap "stopExp; wait; exit 255" SIGINT SIGTERM


# 2 - Experimentation

let e=${#clients[@]}-1
for i in `seq 0 $e`
do
    echo "launching on ${clients[$i]}"
    nohup ${SSHCMD} ${clients[$i]} "${scriptdir}/client.sh" > ${clients[$i]} &
done

wait

