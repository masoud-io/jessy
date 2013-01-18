
scriptdir="/home/msaeidaardekani/nancy/jessy/scripts"

nodes=("msaeidaardekani@access.grid5000.fr")

let e=${#nodes[@]}-1
for i in `seq 0 $e`
do	
#	scp ./grid5kLauncher.sh ${nodes[$i]}:${scriptdir}/grid5kLauncher.sh
#	scp ./experience.sh ${nodes[$i]}:${scriptdir}/experience.sh
#	scp ./configuration.sh ${nodes[$i]}:${scriptdir}/configuration.sh
	
#	scp ./jessy.sh ${nodes[$i]}:${scriptdir}/jessy.sh
#	scp ./concurrentlinkedhashmap.jar ${nodes[$i]}:${scriptdir}/concurrentlinkedhashmap.jar
#	scp ./high-scale-lib.jar ${nodes[$i]}:${scriptdir}/high-scale-lib.jar
	
#	scp ./config.property ${nodes[$i]}:${scriptdir}/config.property
#	scp /local/msaeida/nmsi.tar ${nodes[$i]}:${scriptdir}/nmsi.tar 
#	scp -r ./Loaded_YCSB/4/ ${nodes[$i]}:${scriptdir}/
	scp ../../../jessy.jar ${nodes[$i]}:${scriptdir}/jessy.jar
	scp ../../../fractal.jar ${nodes[$i]}:${scriptdir}/fractal.jar
done

