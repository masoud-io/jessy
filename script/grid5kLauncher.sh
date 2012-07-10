#!/bin/bash

if [ ${#} -lt 2 ]
then
  echo 'grid5kLaucher: usage: this script need at least two parameters: number of clients and number of servers'
  exit
fi

trap "stopExecution"  SIGINT SIGTERM

path=$(pwd)

function stopExecution(){
        
	echo "grid5kLaucher: grid5kLaucher stop. Deleting job..."
	oargriddel $RES_ID
        echo "grid5kLaucher: done"

        exit
}

sed -ie "s#^source.*#source $path/configuration.sh#g" clauncher.sh
sed -ie "s#^source.*#source $path/configuration.sh#g" client.sh 
sed -ie "s#^source.*#source $path/configuration.sh#g" experience.sh
sed -ie "s#^source.*#source $path/configuration.sh#g" jessy.sh
sed -ie "s#^source.*#source $path/configuration.sh#g" launcher.sh
sed -ie "s#^scriptdir=.*#scriptdir=$path#g" configuration.sh


clientsNumber=$1
serversNumber=$2
nodesNumber=$((clientsNumber+serversNumber))

#clientsNumber=2
#serversNumber=2
#nodesNumber=4


echo "grid5kLaucher: reserving $nodesNumber nodes..."
oargridsub -t allow_classic_ssh -w '01:00:00' nancy:rdef="/nodes=$nodesNumber" > tmp
#oargridsub -t allow_classic_ssh -w '00:04:00' nancy:rdef="/nodes=4" > tmp
#oargridsub -t allow_classic_ssh -w '0:20:00' nancy:rdef="/nodes=2", lille:rdef="/nodes=2" > tmp
echo "grid5kLaucher: done"

#retreving batch and grid reservation IDs
RES_ID=$(grep "Grid reservation id" tmp | cut -f2 -d=)
OAR_JOB_KEY_PATH=$(grep "SSH KEY" tmp | cut -b 25-)
#BATCH_ID=$(grep "batchId" tmp | cut -f2 -s -d=)
#BATCH_ID=${BATCH_ID//[[:space:]]/}

#writing names and ip in ./machines file
oargridstat -w -l $RES_ID | sed '/^$/d' | sort | uniq > ./machines

#echo "sleeping 60 sec"
#sleep 60
#echo "getUp"


rm myfractal.xml

echo '<?xml version="1.0" encoding="ISO-8859-1" ?>' >> myfractal.xml
echo '<FRACTAL>'  >> myfractal.xml
echo '<BootstrapIdentity>' >> myfractal.xml
echo '<nodelist>' >> myfractal.xml

nodes='' #'nodes=('
servers='' #'servers=('
clients='' #'clients=('

echo ''

i=0
while read line
do
  host $line > tmp
  name=$(cut tmp -f1 -d ' ')
  ip=$(cut tmp -f4 -d ' ')
 
  nodes="$nodes \"$name\""
  if [ $i -lt $serversNumber ]
  then
    echo 'grid5kLaucher. server: '$name
    echo '<node id="'$i'" ip="'$ip'"/>' >> myfractal.xml
    servers="$servers \"$name\""
  else
    echo 'grid5kLaucher. client: '$name
    clients="$clients \"$name\""
  fi

  i=$((i+1))
done < machines

echo ''

nodes="nodes=("$nodes")"
servers="servers=("$servers")"
clients="clients=("$clients")"

#echo "sleeping 600 sec before remove machines and tmp"
#sleep 600
#echo "getUp"

rm machines tmp

echo '</nodelist>' >> myfractal.xml
echo '</BootstrapIdentity>' >> myfractal.xml
echo '</FRACTAL>' >> myfractal.xml


sed -i "s/nodes=.*/${nodes}/g" configuration.sh
sed -i "s/servers=.*/${servers}/g" configuration.sh
sed -i "s/clients=.*/${clients}/g" configuration.sh

#export OAR_JOB_ID=$BATCH_ID
export OAR_JOB_KEY_FILE=$OAR_JOB_KEY_PATH

echo 'grid5kLaucher: exported oarJobKeyFile ' $OAR_JOB_KEY_PATH

#echo 'grid5kLaucher: synchronizing keys and data...'
#rsync --delete -avz ~/.ssh --exclude known_hosts lille.grid5000.fr:
#rsync --delete -avz ./* lille.grid5000.fr:./jessy/scripts/
#echo 'grid5kLaucher: done'

#echo "sleeping 600 sec before run experience"
#sleep 600
#echo "getUp"


echo "grid5kLaucher: myfractal and configuration.sh are done, lauching experience..."
./experience.sh
echo "grid5kLaucher: done, deleting jobs"

oargriddel $RES_ID