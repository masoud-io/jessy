


node=("msaeidaardekani@access.grid5000.fr")
sites=("lille" "grenoble" "bordeaux" "toulouse" "luxembourg" "sophia" "lyon" "rennes" "reims") #"nancy"
files=("jessy.jar" "fractal.jar")

let e=${#sites[@]}-1
for i in `seq 0 $e`
do	
scriptdir="/home/msaeidaardekani/${sites[i]}/jessy/scripts"
echo "Sending to " ${sites[i]}

	let fc=${#files[@]}-1
	for f in `seq 0 $fc`
	do	
		scp ../../../${files[$f]} $node:${scriptdir}
	done
#		scp -r Loaded_YCSB/4 $node:${scriptdir}
done

