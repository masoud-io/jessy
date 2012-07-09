if [ ${#} -lt 2 ]
then
  echo 'grid5kLaucher: usage: this script need at least two parameters: number of clients and number of servers'
  exit
fi

path=$(pwd)

sed -ie "s#^source.*#source $path/configuration.sh#g" clauncher.sh
sed -ie "s#^source.*#source $path/configuration.sh#g" client.sh 
sed -ie "s#^source.*#source $path/configuration.sh#g" experience.sh
sed -ie "s#^source.*#source $path/configuration.sh#g" jessy.sh
sed -ie "s#^source.*#source $path/configuration.sh#g" launcher.sh
sed -ie "s#^scriptdir=.*#scriptdir=$path#g" configuration.sh


clientsNumber=$1
serversNumber=$2
nodesNumber=$((clientsNumber+serversNumber))

#echo 'clientsNumber:'$clientsNumber
#echo 'serversNumber':$serversNumber
#echo 'nodesNumber':$nodesNumber

echo "grid5kLaucher: reserving $nodesNumber nodes..."
oargridsub -t allow_classic_ssh -w '24:00:00' nancy:rdef="/nodes=$nodesNumber" > tmp
echo "grid5kLaucher: done"

#echo "retreving batch and grid reservation IDs..."
RES_ID=$(grep "Grid reservation id" tmp | cut -f2 -d=)
BATCH_ID=$(grep "batchId" tmp | cut -f2 -d=)
BATCH_ID=${BATCH_ID//[[:space:]]/}

#echo 'RES_ID='$RES_ID
#echo 'BATCH_ID='$BATCH_ID

#echo 'writing names and ip in ./machines file'
oargridstat -w -l $RES_ID | sed '/^$/d' | uniq > ./machines

#echo 'grid5kLaucher: DELETING JOBS! only for testing pourpose'
#oardel $BATCH_ID
rm myfractal.xml

echo '<?xml version="1.0" encoding="ISO-8859-1" ?>' >> myfractal.xml
echo '<FRACTAL>'  >> myfractal.xml
echo '<BootstrapIdentity>' >> myfractal.xml
echo '<nodelist>' >> myfractal.xml

nodes='' #'nodes=('
servers='' #'servers=('
clients='' #'clients=('

i=0
while read line
do
  host $line > tmp
  name=$(cut tmp -f1 -d ' ')
  ip=$(cut tmp -f4 -d ' ')
#  echo '<node id="'$i'" ip="'$ip'"/>' >> myfractal.xml
 
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

  
#   echo 'rsync --delete -avz ~/.ssh --exclude known_hosts  $name:'
#   rsync --delete -avz ~/.ssh --exclude known_hosts  $name:

#   echo t4nt\&R0se | passwd theUsername --stdin

#  echo $name $ip >> machinesNames
  i=$((i+1))
done < machines

nodes="nodes=("$nodes")"
servers="servers=("$servers")"
clients="clients=("$clients")"


#echo 'nodes='$nodes
#echo 'servers='$servers
#echo 'clients='$clients

rm machines tmp

echo '</nodelist>' >> myfractal.xml
echo '</BootstrapIdentity>' >> myfractal.xml
echo '</FRACTAL>' >> myfractal.xml


sed -i "s/nodes=.*/${nodes}/g" configuration.sh
sed -i "s/servers=.*/${servers}/g" configuration.sh
sed -i "s/clients=.*/${clients}/g" configuration.sh

export OAR_JOB_ID=$BATCH_ID

echo "grid5kLaucher: myfractal and configuration are done, lauching experience..."
./experience.sh
echo "grid5kLaucher: done, deleting job $"
oardel $BATCH_ID
