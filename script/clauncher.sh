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

function stopTimer(){
        kill $TIMERPID
}
trap "echo 'Clauncher catches Quit Signal'; stopTimer; stopExp; wait; exit 255" SIGINT SIGTERM

#Trap timeout
trap "echo 'Clauncher catches Timeout'; stopExp; wait; exit 3" SIGUSR1

# 2 - Experimentation

let e=${#clients[@]}-1
for i in `seq 0 $e`
do
    echo "launching on ${clients[$i]}"
    nohup ${SSHCMD} ${clients[$i]} "${scriptdir}/client.sh" > ${clients[$i]} &
    CLIENTPID[$i]=$!
done

export CLIENTPID

export CLAUNCHERPID=$$

#Run the timeout. clauncherTimeout should be set in the configuratio file.
(sleep $clauncherTimeout ; kill -SIGUSR1 $CLAUNCHERPID) &

TIMERPID=$!
export TIMERPID

#Wait normally for all clients to finish
wait ${CLIENTPID[*]}
echo "Clauncher finishes successfully. Returning to experience."

stopTimer
exit 0
