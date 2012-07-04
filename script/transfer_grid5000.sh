
scriptdir="/home/msaeidaardekani/nancy/jessy_grid5000"

nodes=("msaeidaardekani@access.grid5000.fr")

let e=${#nodes[@]}-1
for i in `seq 0 $e`
do	
	scp ../../../jessy.jar ${nodes[$i]}:${scriptdir}/jessy.jar
	scp ../lib/fractal.jar ${nodes[$i]}:${scriptdir}/fractal.jar
done

