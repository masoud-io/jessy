#!/bin/bash

set -x

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
increment=0

next=$(date '+%Y-%m-%d %H:%M:%S')

while [  $reservationFail == "true" ]; do
        
	echo "trying to reserve nodes at... ""$next"

	oargridsub -w '0:05:00' $reservation -s "$next" > tmp

	#retreving batch and grid reservation IDs
	RES_ID=$(grep "Grid reservation id" tmp | cut -f2 -d=)
	OAR_JOB_KEY_PATH=$(grep "SSH KEY" tmp | cut -b 25-)

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
		#reservation="$reservation ${param[$next]}:rdef=/nodes=$nodesNumber,"
		reservation="$reservation ${param[$next]}:rdef=/nodes=$nodesNumber/core=4,"

		next=$(($next+3))
		i=$(($i+1))
        done

reservation=${reservation#?}
reservation=${reservation%?}

echo "starting grid5kLaucher..."
echo ""
#echo "reserving nodes..."
reserveNodes

#oargridsub -w '0:05:00' $reservation > tmp
#echo "done"
#
#retreving batch and grid reservation IDs
#RES_ID=$(grep "Grid reservation id" tmp | cut -f2 -d=)
#OAR_JOB_KEY_PATH=$(grep "SSH KEY" tmp | cut -b 25-)




rm myfractal.xml

echo '<?xml version="1.0" encoding="ISO-8859-1" ?>' >> myfractal.xml
echo '<FRACTAL>'  >> myfractal.xml
echo '<BootstrapIdentity>' >> myfractal.xml
echo '<nodelist>' >> myfractal.xml

echo 'Grid reservation id: ' $RES_ID

nodeStr='' #'nodes=('
servers='' #'servers=('
clients='' #'clients=('
nodes=''

j=0
next=0
for i in `seq 1 $clustersNumber`;
do
        reservation="$reservation ${param[$next]}:rdef=/nodes=$nodes,"

	nodeName=${param[$next]}
	serverNumber=${param[$next+1]}
    clientNumber=${param[$next+2]}

	echo ""
	echo "**********************"
	echo "* deploy on "$nodeName" *"
	echo "**********************"
	echo "server: "$serverNumber
	echo "client: "$clientNumber
	echo ""

	oargridstat -w -l $RES_ID -c $nodeName | sed '/^$/d' | sort | uniq > ./machines

        next=$(($next+3))
	k=0
	while read line
	do
		host $line > tmp
		name=$(cut tmp -f1 -d ' ')
		ip=$(cut tmp -f4 -d ' ')
 
		nodes="$nodes \"$name\""

		if [ $k -lt $serverNumber ]
		then
			echo 'server: '$name
   			echo '<node id="'$j'" ip="'$ip'"/>' >> myfractal.xml
			servers="$servers \"$name\""
		else
		    echo 'client: '$name
		    clients="$clients \"$name\""
		fi
		j=$((j+1))
		k=$((k+1))
	done < machines
done
echo ""

nodeStr="nodes=("$nodes")"
servers="servers=("$servers")"
clients="clients=("$clients")"

echo '</nodelist>' >> myfractal.xml
echo '</BootstrapIdentity>' >> myfractal.xml
echo '</FRACTAL>' >> myfractal.xml
echo "fractal configuration file is done"

sed -i "s/nodes=.*/${nodeStr}/g" configuration.sh
sed -i "s/servers=.*/${servers}/g" configuration.sh
sed -i "s/clients=.*/${clients}/g" configuration.sh
echo "configuration.sh file is done"

rm machines tmp

export OAR_JOB_KEY_FILE=$OAR_JOB_KEY_PATH

echo 'exported oarJobKeyFile ' $OAR_JOB_KEY_PATH

next=0
echo 'synchronizing keys and data...'
for i in `seq 1 $clustersNumber`;
do
	nodeName=${param[$next]}
	echo "synchronizing "$nodeName"..."

	rsync -a -f"+ */" -f"- *" ../../jessy/scripts $nodeName.grid5000.fr:~/jessy	

	rsync --delete -az ./* $nodeName.grid5000.fr:~/jessy/scripts/

	next=$(($next+3))
done


echo ""
echo "**************************************************************************************"
echo "*** grid5kLaucher: myfractal and configuration.sh are done, launching experience... ***"
echo "**************************************************************************************"

./experience.sh ${param[*]}
echo "******************************************************************************"
echo "grid5kLaucher: done, deleting jobs"
echo "******************************************************************************"

oargriddel $RES_ID
