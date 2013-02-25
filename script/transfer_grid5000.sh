


node=("msaeidaardekani@access.grid5000.fr")
sites=("nancy" "bordeaux" "lille" "grenoble" "toulouse" "luxembourg" "sophia" "lyon" "rennes" "reims") #"nancy"
#jarfiles=("jessy.jar" "fractal.jar")
#shfiles=("experience.sh" "jessy.sh")

let e=${#sites[@]}-1
for i in `seq 0 $e`
do	
scriptdir="/home/msaeidaardekani/${sites[i]}/jessy/scripts"

echo "Deleting output files"
ssh msaeidaardekani@access.grid5000.fr rm -f /home/msaeidaardekani/${sites[i]}/jessy/scripts/*.fr*

echo "Sending to " ${sites[i]}

	let fc=${#jarfiles[@]}-1
	for f in `seq 0 $fc`
	do	
		scp ../../../${jarfiles[$f]} $node:${scriptdir}
	done

	let fc=${#shfiles[@]}-1
	for f in `seq 0 $fc`
	do	
		scp ./${shfiles[$f]} $node:${scriptdir}
	done

#	scp ../config.property $node:${scriptdir}

#		scp -r Loaded_YCSB/Sequential/6 $node:${scriptdir}
done

