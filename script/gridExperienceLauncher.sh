#!/bin/bash

#sed -ie "s#^source.*#source $path/gridExperienceConfiguration.sh#g" gridExperienceConfiguration.sh
source gridExperienceConfiguration.sh

function run {

		servers=`seq ${minServers} ${serverIncrement} ${maxServers}`
		#iterate over servers
		for s in ${servers}; 
		do
			#echo $s" servers"
			cLen=${#clusterInUse[@]}

			if [[ "$avoidUnbalancedRuns" == "true" && $s -lt $cLen ]]; then
				echo "WARNING skipping run with "$s" servers on "$cLen " clusters"
			else
				serverPerCluster=$(( $s/$cLen ))
				clients=`seq ${minClientsForEachServer} ${clientIncrement} ${maxClientsForEachServer}`
				#iterate over clients
				for c in ${clients}; 
				do
					rest=$(( $serverPerCluster*$cLen ))
					rest=$(( $s-$rest ))

					if [[ "$avoidUnbalancedRuns" == "true" &&  $rest -gt 0 ]]; then
						echo "WARNING skipping unbalanced run with "$s" servers on "$cLen " clusters"
					else
						for ciu in "${clusterInUse[@]}"
						do
							serverForThisLaunch=$serverPerCluster
							if [ $rest -gt 0 ]; then
								((serverForThisLaunch++))
								rest=$(( $rest - 1 ))
							fi
							clientForCluster=$(( $c*$serverForThisLaunch ))
							if [[ $clientForCluster == 0 && $serverForThisLaunch == 0 ]]; then
								clientForCluster=1;
								echo "Running one extra client in "$ciu" to avoid run with 0 servers and 0 clients"
							fi
							launchCommand="$launchCommand $ciu $serverForThisLaunch $clientForCluster"
						done
					fi
				echo -e "calling grid5kLauncher on " $launchCommand" ...\n"
				./grid5kLauncher.sh $launchCommand
				echo "done."
				launchCommand=""
				done
			fi
		done
}

clusterCombination=${#clusters[@]}

serverCombinations=$((  $maxServers - $minServers + 1 ))
serverCombinations=$(( $serverCombinations /  $serverIncrement ))

clientCombinations=$(( $maxClientsForEachServer - $minClientsForEachServer +1 ))
clientCombinations=$(( $clientCombinations /  $clientIncrement ))

if [ $varyClusterDeployment == "true" ]; then
	totalCombinations=$(( $clusterCombination * $serverCombinations * $clientCombinations ))
else
	totalCombinations=$(( $serverCombinations * $clientCombinations ))
fi

read -p "with this configuration there will be generated around $totalCombinations runs on the grid. Are you sure to continue? " -n 1 -r
if [[ $REPLY =~ ^[Nn]$ ]]
then
	echo ""
   exit 
fi


echo ""
if [ "$varyClusterDeployment" == "true" ]; then
#iterate over clusters
	i=-1
	for cluster in "${clusters[@]}"
	do
		i=$(( $i+1 ))
		#clusters used for this execution
		clusterInUse[$i]=$cluster
		run
	done
else
	clusterInUse=("${clusters[@]}")
	run
fi

