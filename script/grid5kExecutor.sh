#!/bin/bash


RES_ID=$(grep "Grid reservation id" tmpOar | cut -f2 -d=)

export clustersNumber=$(($# / 3))

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

export param=("$@")
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

        oargridstat -w -l $RES_ID | grep $nodeName | sed '/^$/d' | sort | uniq > ./machines

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

#rm machines tmp

export OAR_JOB_KEY_FILE=`cat OAR_JOB_KEY_PATH`

echo 'exported oarJobKeyFile ' $OAR_JOB_KEY_FILE

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

