
source /local/msaeida/workspace/Implementations/jessy/script/configuration.sh

nodes=("msaeida@cluster.lip6.fr")

let e=${#nodes[@]}-1
for i in `seq 0 $e`
do
	
	#scp ../config.property ${nodes[$i]}:${scriptdir}/config.property
	#scp ../log4j.properties ${nodes[$i]}:${scriptdir}/log4j.properties
	#scp ../myfractal.xml ${nodes[$i]}:${scriptdir}/myfractal.xml
	#scp ./configuration.sh ${nodes[$i]}:${scriptdir}/configuration.sh
    #scp ./jessy.sh ${nodes[$i]}:${scriptdir}/jessy.sh
    #scp ./launcher.sh ${nodes[$i]}:${scriptdir}/launcher.sh
    #scp ./clauncher.sh ${nodes[$i]}:${scriptdir}/clauncher.sh
    #scp ./client.sh ${nodes[$i]}:${scriptdir}/client.sh
#        scp ./experience.sh ${nodes[$i]}:${scriptdir}/experience.sh
	#scp ../config/YCSB/workloads/${workloadName} ${nodes[$i]}:${scriptdir}/${workloadName}
	scp ../../../jessy.jar ${nodes[$i]}:${scriptdir}/jessy.jar
	scp ../../../fractal.jar ${nodes[$i]}:${scriptdir}/fractal.jar
done

