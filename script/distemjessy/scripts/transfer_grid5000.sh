


node=("$1@access-north.grid5000.fr")
sites=("lille") # "toulouse" "bordeaux" "rennes" "sophia" "grenoble") # "luxembourg" "lyon" "reims") 
#shfiles=("experience.sh" "jessy.sh")
#jarfiles=(`ls *.jar | tr '\n' ' '`)
#shfiles=(`ls *.sh | tr '\n' ' '`)
#dirs=("Loaded_YCSB/4") # "Loaded_YCSB/Sequential/3" "Loaded_YCSB/Sequential/4" "config")

let e=${#sites[@]}-1
for i in `seq 0 $e`
do	
scriptdir="/home/$1/${sites[i]}/distemjessy/scripts"

echo "Deleting output files"
ssh $1@access-north.grid5000.fr rm -rf /home/$1/${sites[i]}/jessy/scripts/*.fr*

echo "Sending to " ${sites[i]}

	let fc=${#jarfiles[@]}-1
	for f in `seq 0 $fc`
	do	
		scp ./${jarfiles[$f]} $node:${scriptdir}
	done

	let fc=${#shfiles[@]}-1
	for f in `seq 0 $fc`
	do	
		scp ./${shfiles[$f]} $node:${scriptdir}
	done

        let fc=${#dirs[@]}-1
        for f in `seq 0 $fc`
        do
		scp -r ${dirs[$f]} $node:${scriptdir}
        done


#	scp ../config.property $node:${scriptdir}
#	scp log4j.properties $node:${scriptdir}


	scp ../../jessy.jar $node:${scriptdir}
#	scp ../../Batelier/target/batelier-0.0.1-SNAPSHOT.jar $node:${scriptdir}/fractal.jar

#		scp -v -r Loaded_YCSB/4/serrano_sv_gc $node:${scriptdir}/4/
#		scp -v -r Loaded_YCSB/4/sdur_vv_gc $node:${scriptdir}/4/
done

