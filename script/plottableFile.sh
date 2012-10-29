#!/bin/sh

#this script generate a plottable file with one row for each measurement file (mes extension)
#and one column for each configuration parameter and input measurement parameter

if [ $# -eq 0 ]
  then
    echo "No arguments supplied."
	echo "This script expect a list of measurements parameter as input" 
	exit 1
fi

for par in "$@"
do
	parameters=$parameters"\t"$par
done

echo "# Consistency\tServer_Machines\tClient_Machines\tNumber_Of_Clients$parameters\n" > output

for f in *.mes; do 

line=$(sed -n "/Consistency,/p" $f);
consistency=`echo $line | gawk -F',' '{print $2}'`;

line=$(sed -n "/Server_Machines,/p" $f);
sm=`echo $line | gawk -F',' '{print $2}'`;

line=$(sed -n "/Client_Machines,/p" $f);
cm=`echo $line | gawk -F',' '{print $2}'`;

line=$(sed -n "/Number_Of_Clients,/p" $f);
noc=`echo $line | gawk -F',' '{print $2}'`;

configParameters=$consistency"\t"$sm"\t"$cm"\t"$noc

echo "configParameters" $configParameters

#	echo "Processing $f file..";	
	for par in "$@"
	do
	#escape all special carachters
		par=$(echo "$par"|sed 's!\([]\*\$\/&[]\)!\\\1!g')
		echo "Processing $par parameter..";
		execLine=$(sed -n "/$par/p" $f)
		execParam=`echo $execLine | gawk -F',' '{print $3}'`;
		executionParameters=$executionParameters"\t"$execParam
	done
echo $configParameters$executionParameters >> output
executionParameters=""
done


