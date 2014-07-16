#!/bin/bash

#this script generate a plottable file with one row for each measurement file (mes extension)
#and one column for each configuration parameter and input measurement parameter

outputFile="plottable.txt"

echo "building plottable file "$outputFile"..."

if [ $# -eq 0 ]
  then
    
	echo "No arguments supplied, using default (all measurements)"
	
	args[0]="[OVERALL], average RunTime(ms)";
	args[1]="[OVERALL], Throughput(ops/sec)";
	args[2]="[ UPDATE ], AverageLatency(ms)";
	args[3]="[ UPDATE ], 95thPercentileLatency(ms)";
	args[4]="[ UPDATE ], 99thPercentileLatency(ms)";
	args[5]="[ READ ], AverageLatency(ms)";
	args[6]="[ READ ], 95thPercentileLatency(ms)";
	args[7]="[ READ ], 99thPercentileLatency(ms)";

fi

if [ $# -gt 0 ]
  then
	i=0;
	for par in "$@"
	do
		args[$i]=$par;
		i=$(echo "scale=5;$i + 1" | bc);
	done
fi

#for par in "$@"
for par in "${args[@]}"
do
	parameters=$parameters"\t"$par
done

echo -e "# Consistency\tServer_Machines\tClient_Machines\tNumber_Of_Clients$parameters\n" > $outputFile

for f in *.mes; do 

# the script expect a comma after each measurement, if the comma is not ther it will be added before processing the file 
if ! grep -q "\[OVERALL\], average RunTime(ms)," "${f}" ; then
	sed -i 's/\[OVERALL\], average RunTime(ms)/\[OVERALL\], average RunTime(ms),/g' ${f}
fi

if ! grep -q "\[OVERALL\], Throughput(ops\/sec)," "${f}" ; then
	sed -i 's/\[OVERALL\], Throughput(ops\/sec)/\[OVERALL\], Throughput(ops\/sec),/g' ${f}
fi

	line=$(sed -n "/Consistency:/p" $f);
	consistency=`echo $line | gawk -F':' '{print $2}' | gawk '$1=$1'`;

	line=$(sed -n "/Server_Machines:/p" $f);
	sm=`echo $line | gawk -F':' '{print $2}' | gawk '$1=$1'`;

	line=$(sed -n "/Client_Machines:/p" $f);
	cm=`echo $line | gawk -F':' '{print $2}' | gawk '$1=$1'`;

	line=$(sed -n "/Number_Of_Clients:/p" $f);
	noc=`echo $line | gawk -F':' '{print $2}' | gawk '$1=$1'`;

	#configParameters="\t\t"$consistency"\t\t\t\t"$sm"\t\t\t\t"$cm"\t\t\t\t"$noc
	configParameters=$consistency"\t"$sm"\t"$cm"\t"$noc

	for par in "${args[@]}"
	do
	#escape all special carachters
		par=$(echo "$par"|sed 's!\([]\*\$\/&[]\)!\\\1!g')
		execLine=$(sed -n "/$par/p" $f)
		execParam=`echo -e $execLine | gawk -F',' '{print $3}' | gawk '$1=$1'`;
		#executionParameters=$executionParameters"\t\t\t\t\t"$execParam
		executionParameters=$executionParameters"\t"$execParam
	done
echo -e $configParameters$executionParameters >> $outputFile
executionParameters=""
done

echo "done."


