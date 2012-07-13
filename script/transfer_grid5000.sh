
scriptdir="/home/msaeidaardekani/nancy/script_grid5k"

nodes=("msaeidaardekani@access.grid5000.fr")

let e=${#nodes[@]}-1
for i in `seq 0 $e`
do	
	scp ./grid5kLauncher.sh ${nodes[$i]}:${scriptdir}/grid5kLauncher.sh
	scp ./experience.sh ${nodes[$i]}:${scriptdir}/experience.sh
	scp ./configuration.sh ${nodes[$i]}:${scriptdir}/configuration.sh
	scp ../../jessy.jar ${nodes[$i]}:${scriptdir}/jessy.jar
	scp ../../fractal.jar ${nodes[$i]}:${scriptdir}/fractal.jar
done

