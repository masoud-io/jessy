
#source /local/msaeida/workspace/Implementations/jessy/script/configuration.sh
scriptdir="/home/rioc/saeidaar/jessy_script"
nodes=("saeidaar@rioc.inria.fr")

let e=${#nodes[@]}-1
for i in `seq 0 $e`
do
#scp -r *.jar ${node[$i]}:${scriptdir}
#scp *.sh ${node[$i]}:${scriptdir}	
#scp -r config ${node[$i]}:${scriptdir}

	scp ../../Batelier/target/batelier-0.0.1-SNAPSHOT.jar ${nodes[$i]}:${scriptdir}/fractal.jar
	scp ../../../jessy.jar ${nodes[$i]}:${scriptdir}/jessy.jar
done

