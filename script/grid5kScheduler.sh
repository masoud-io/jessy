#!/bin/bash

rm -f *.fr
running_on_grid=true

if [ ${#} -lt 3 ]
then
  echo 'grid5kLaucher: usage: this script need at least three parameters: cluster name, number of clients and number of servers'
  exit
fi

trap "stopExecution"  SIGINT SIGTERM

path=$(pwd)

function stopExecution(){
        
        echo "grid5kLaucher: grid5kLaucher stop. Deleting jobs..."
        oargriddel $RES_ID
        echo "grid5kLaucher: done"

        exit
}

function reserveNodes(){

reservationFail=true;

increment=3566

next=$(date '+%Y-%m-%d %H:%M:%S')

while [  $reservationFail == "true" ]; do
        
        echo "trying to reserve nodes at... ""$next"

        oargridsub -w '20:00:00' $reservation -s "$next" > tmpOar

        #retreving batch and grid reservation IDs
        RES_ID=$(grep "Grid reservation id" tmpOar | cut -f2 -d=)
        echo $(grep "SSH KEY" tmpOar | cut -b 25-) > OAR_JOB_KEY_PATH

        if [ -z "$RES_ID" ]
        then
                increment=$(($increment+30))
                next=$(date '+%Y-%m-%d %H:%M:%S' --date=' +'$increment' minutes')
                next=${next//\"/}
                next=${next//\'/}
                echo 'clusters unavailable now, trying to reserve at... '$next
                echo ""
        else
        
                now=$(date +%s)
                next=$(date -d "$next" +%s)
                next=${next//\"/}
                next=${next//\'/}
                timeToWait=$(( $next - $now ))
                minutes=$(($timeToWait / 60))
                
                cp tmpOar "tmpOar_$RES_ID"
				cp OAR_JOB_KEY_PATH "OAR_JOB_KEY_PATH_$RES_ID"

                echo "I will sleep for:" $minutes "minutes" #$(date -d @$timeToWait)
                sleep $timeToWait

            reservationFail=false
        fi
done

echo "done"

}


sed -ie "s#^source.*#source $path/configuration.sh#g" clauncher.sh
sed -ie "s#^source.*#source $path/configuration.sh#g" console.sh
sed -ie "s#^source.*#source $path/configuration.sh#g" client.sh
sed -ie "s#^source.*#source $path/configuration.sh#g" experience.sh
sed -ie "s#^source.*#source $path/configuration.sh#g" jessy.sh
sed -ie "s#^source.*#source $path/configuration.sh#g" launcher.sh
sed -ie "s#^scriptdir=.*#scriptdir=$path#g" configuration.sh


export clustersNumber=$(($# / 3))

export param=("$@")
next=0
i=0
reservation="";



next=0
for i in `seq 1 $clustersNumber`;
        do
                clusters[$i]=${param[$next]}
                nodesNumber=$((${param[$next+1]}+${param[$next+2]}))
                reservation="$reservation ${param[$next]}:rdef=/nodes=$nodesNumber/core=4,"

                next=$(($next+3))
                i=$(($i+1))
        done

reservation=${reservation#?}
reservation=${reservation%?}

echo "Reserving Nodes..."
echo ""
reserveNodes

