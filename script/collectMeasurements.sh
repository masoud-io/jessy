#!/bin/bash

# take as input all clients and servers output file, 
# give in output two files: clientsAggregation and 
# serversAggregation containing the aggregation of 
# client and server side measurements

source ./configuration.sh
outputFilename="measurements"$(date +"%m-%dat%Hh%Mm%Ss")".mes"

#collect measurements strings from all clients output files in commandsArray
function collectMeasurementStrings(){

	parameterIndex=0;

	let e=${#clients[@]}-1
	for i in `seq 0 $e`
	do
		client=${clients[$i]}
		clientMeasurements=$client".sterr"
		while read line
		do
			if [[ $line == '['*']'* ]]; then
				findedCommand=$(echo $line | cut -d'[' -f2|cut -d']' -f1);
				new=true;
				case "${commandsArray[@]}" in  *$findedCommand*) new=false
						;; 
				esac
				if $new ; then 
					#OVERALL case is threated apart
					if [ $findedCommand != "OVERALL" ]; then
						commandsArray[$parameterIndex]=$findedCommand;
						parameterIndex=$parameterIndex+1;
					fi
				fi					
			fi
		done < $clientMeasurements
	done

#echo "commandsArray: "${commandsArray[@]}
}

#this function sum field's results without divide
function collectMeasurements(){

	runTime=0;
	throughput=0;

	propertyIndex=-1;
	arrayIndex=1;

	let e=${#clients[@]}-1
	for i in `seq 0 $e`
	do
		client=${clients[$i]}
		
		setted=0;
		clientMeasurements=$client".sterr"

		echo "processing "$clientMeasurements" file for [overall] measurements"

		while read line
		do
			if [[ $line == *"[OVERALL], RunTime(ms)"* ]]; then
				time=`echo $line | gawk -F',' '{print $3}'`;
				runTime=$(echo "scale=5;$runTime+$time" | bc);
			fi

			if [[ $line == *"[OVERALL], Throughput(ops/sec)"* ]]; then
				th=`echo $line | gawk -F',' '{print $3}'`;
				throughput=$(echo "scale=5;$throughput+$th" | bc);
			fi
		done < $clientMeasurements
	done

	clientsNumber=$e+1

	runTime=$(echo "scale=5;$runTime/$clientsNumber" | bc);

	for command in "${commandsArray[@]}"
	do
		commandOccourrence=0;
		propertyIndex=$(($propertyIndex+1));
		arrayIndex=$(($propertyIndex * 10));
#		echo "propertyIndex:" $propertyIndex
#		echo "-----------------------------"
#		echo "external loop"
#		echo "processing " $command
#		echo "arrayIndex:" $arrayIndex
#		echo "array: " ${commandsLongArray[@]}
#		echo "-----------------------------"

		commandsLongArray[arrayIndex]=0;
		commandsLongArray[arrayIndex+1]=0;
		commandsLongArray[arrayIndex+2]=0;
		commandsLongArray[arrayIndex+3]=0;
		commandsLongArray[arrayIndex+4]=0;
		commandsLongArray[arrayIndex+5]=0;
		commandsLongArray[arrayIndex+6]=0;
		commandsLongArray[arrayIndex+7]=0;
		commandsLongArray[arrayIndex+8]=0;
		commandsLongArray[arrayIndex+9]=0;


		let e=${#clients[@]}-1
		for i in `seq 0 $e`
		do
			client=${clients[$i]}
		
			setted=0;
			clientMeasurements=$client".sterr"

			while read line
			do

				if [[ $line == *$command* ]]; then

					if [[ $setted == 0 ]]; then 
						commandOccourrence=$((commandOccourrence + 1));
						setted=1;
					fi
					case "$line" in

						*"Operations"*)
							operations=`echo $line | gawk -F',' '{print $3}'`;
							commandsLongArray[arrayIndex+1]=$(echo "scale=5;${commandsLongArray[$arrayIndex+1] + ${operations}}" | bc);
							#echo "using index" $(($arrayIndex + 1)) ", operations: " $operations;
					    ;;
						*"AverageLatency"*)
							latency=`echo $line | gawk -F',' '{print $3}'`;
							commandsLongArray[arrayIndex+2]=$(echo "scale=5;${commandsLongArray[$arrayIndex+2] + ${latency}} " | bc);
							#echo "using index" $(($arrayIndex + 2)) ", latency: " $latency ;
					    ;;
						*"MinLatency"*)
							minLatency=`echo $line | gawk -F',' '{print $3}'`;
							commandsLongArray[arrayIndex+3]=$(echo "scale=5;${commandsLongArray[$arrayIndex+3] + ${minLatency}}" | bc);
							#echo "using index" $(($arrayIndex + 3)) ", minLatency: " $minLatency ;
					    ;;
						*"MaxLatency"*)
							maxLatency=`echo $line | gawk -F',' '{print $3}'`;
							commandsLongArray[arrayIndex+4]=$(echo "scale=5;${commandsLongArray[$arrayIndex+4] + ${maxLatency}}" | bc);
							#echo "using index" $(($arrayIndex + 4)) ", maxLatency: " $maxLatency ;
					    ;;
						*"95thPercentileLatency"*)
							NFthPercentileLatency=`echo $line | gawk -F',' '{print $3}'`;
							commandsLongArray[arrayIndex+5]=$(echo "scale=5;${commandsLongArray[$arrayIndex+5] + ${NFthPercentileLatency}}" | bc);
							#echo "using index" $(($arrayIndex + 5)) ", NFthPercentileLatency: " $NFthPercentileLatency ;
					    ;;
						*"99thPercentileLatency"*)
							NNthPercentileLatency=`echo $line | gawk -F',' '{print $3}'`;
							commandsLongArray[arrayIndex+6]=$(echo "scale=5;${commandsLongArray[$arrayIndex+6] + ${NNthPercentileLatency}}" | bc);
							#echo "using index" $(($arrayIndex + 6)) ", NNthPercentileLatency: " $NNthPercentileLatency ;
					    ;;
						*"Return=0"*)
							return=`echo $line | gawk -F',' '{print $3}'`;
							commandsLongArray[arrayIndex+7]=$(echo "scale=5;${commandsLongArray[$arrayIndex+7] + ${return}}" | bc);
							#echo "using index" $(($arrayIndex + 6)) ", NNthPercentileLatency: " $NNthPercentileLatency ;
					    ;;
						*"1000"*)
							mille=`echo $line | gawk -F',' '{print $3}'`;
							commandsLongArray[arrayIndex+8]=$(echo "scale=5;${commandsLongArray[$arrayIndex+8] + ${mille}}" | bc);
							#echo "using index" $(($arrayIndex + 7)) ", mille: " $mille ;
					    ;;
					esac
				fi
			done < $clientMeasurements
			commandsLongArray[arrayIndex]=$command;
			commandsLongArray[arrayIndex+9]=${commandOccourrence};

		done
	done
#echo "finalArray: " ${commandsLongArray[@]}
}

#build a file with the average of all measurements from the commandsLongArray
function printClientsMeasurements(){

#parameters

c=${#clients[@]};
runTime=$(echo "scale=5;$runTime / $c" | bc);

echo "************** CLIENT SIDE MEASUREMENTS  **************" >> $outputFilename;
echo "" >> $outputFilename;
echo "[OVERALL], average RunTime(ms)" $runTime >> $outputFilename;
echo "[OVERALL], Throughput(ops/sec)" $throughput >> $outputFilename;
echo "" >> $outputFilename;

items=${#commandsArray[@]}

for ((item=0 ; item < $items ; item++))
do

	pointer=$item*10;

	commandName=${commandsLongArray[$pointer]}
	operations=${commandsLongArray[$pointer+1]}
	avgltc=${commandsLongArray[$pointer+2]}
	minl=${commandsLongArray[$pointer+3]}
	maxl=${commandsLongArray[$pointer+4]}
	nfpc=${commandsLongArray[$pointer+5]}
	nnpc=${commandsLongArray[$pointer+6]}
	retz=${commandsLongArray[$pointer+7]}
	gt=${commandsLongArray[$pointer+8]}
	factor=${commandsLongArray[$pointer+9]}

	avgltc=$(echo "scale=5;$avgltc / $factor" | bc);
	minl=$(echo "scale=5;$minl / $factor" | bc);
	maxl=$(echo "scale=5;$maxl / $factor" | bc);
	nfpc=$(echo "scale=5;$nfpc / $factor" | bc);
	nnpc=$(echo "scale=5;$nnpc / $factor" | bc);

	echo "[" ${commandName} "], operations, " ${operations} >> $outputFilename;
	echo "[" ${commandName} "], AverageLatency(ms), " ${avgltc} >> $outputFilename;
	echo "[" ${commandName} "], MinLatency(ms), " ${minl} >> $outputFilename;
	echo "[" ${commandName} "], MaxLatency(ms), " ${maxl} >> $outputFilename;
	echo "[" ${commandName} "], 95thPercentileLatency(ms), " ${nfpc} >> $outputFilename;
	echo "[" ${commandName} "], 99thPercentileLatency(ms), " ${nnpc} >> $outputFilename;
	echo "[" ${commandName} "], Return=0, " ${retz} >> $outputFilename;
	echo "[" ${commandName} "], >1000, " ${gt} >> $outputFilename;
	echo "" >> $outputFilename;

done
}


function collectServersMeasurements(){
    overallThroughput=0;
    committedThroughput=0;
    runtime=0;
    updateLatency=0
    readLatency=0
    failedTerminationRatio=0;
    failedExecutionRatio=0;
    failedReadsRatio=0;
    timeoutRatio=0;
    executionTime=0;
    terminationTime=0;
    certificationTime=0;

	if ! [ -s "${scriptdir}/results/${servercount}.txt" ]; then
	    echo -e  "Consistency\tServer_Machines\tClient_Machines\tNumber_Of_Clients\tOverall_Throughput\tCommitted_Throughput\tupdateLatency\treadLatency\tFailed_Termination_Ratio\tFailed_Execution_Ratio\tFailed_Read_Ratio\tTermination_Timeout_Ratio\tTransaction_Execution_Time\tTransaction_Termination_Time\tCertification_Time"
	fi

    let scount=${#servers[@]}-1
    for j in `seq 0 $scount`
    do
	server=${servers[$j]}

	tmp=`grep -a "certificationTime" ${scriptdir}/${server} | gawk -F':' '{print $2}'`;
	if [ -n "${tmp}" ]; then
	    certificationTime=`echo "${tmp}+${certificationTime}"| sed 's/E/*10^/g'`;	    
	fi

    done


    let e=${#clients[@]}-1
    for i in `seq 0 $e`
    do

	client=${clients[$i]}

	tmp=`grep -a Throughput ${scriptdir}/${client} | gawk -F',' '{print $3}'`;
	if [ -n "${tmp}" ]; then
	    overallThroughput=`echo "${tmp} + ${overallThroughput}" | bc`;
	fi

	runtime=`grep -a RunTime ${scriptdir}/${client} | gawk -F',' '{print $3}'`;
	tmp=`grep -a "Return=0" ${scriptdir}/${client} | awk -F "," '{sum+= $3} END {print sum}'`;
	if [ -n "${tmp}" ]; then
	    committedThroughput=`echo "((1000*${tmp})/${runtime}) + ${committedThroughput}" | bc`;
	fi

	tmp=`grep -a "\[UPDATE\], AverageLatency" ${scriptdir}/${client} | gawk -F',' '{print $3}'`;
	if [ -n "${tmp}" ]; then
	    updateLatency=`echo "${tmp} + ${updateLatency}" | bc`;
	fi

	tmp=`grep -a "\[READ\], AverageLatency" ${scriptdir}/${client} | gawk -F',' '{print $3}'`;
	if [ -n "${tmp}" ]; then
	    readLatency=`echo "${tmp} + ${readLatency}" | bc`;
	fi
	
	tmp=`grep -a "ratioFailedTermination" ${scriptdir}/${client} | gawk -F':' '{print $2}'`;
	if [[ (! ${tmp} =~ '/') && (-n "${tmp}") ]]; then
	    failedTerminationRatio=`echo "${tmp}+${failedTerminationRatio}"| sed 's/E/*10^/g'` ;	    
	fi

	tmp=`grep -a "ratioFailedExecution" ${scriptdir}/${client} | gawk -F':' '{print $2}'`;
	if [[ (! ${tmp} =~ '/') && (-n "${tmp}") ]]; then
	    failedExecutionRatio=`echo "${tmp}+${failedExecutionRatio}"| sed 's/E/*10^/g'` ;	    
	fi


	tmp=`grep -a "ratioFailedReads" ${scriptdir}/${client} | gawk -F':' '{print $2}'`;
	if [[ (! ${tmp} =~ '/') && (-n "${tmp}") ]]; then
	    failedReadsRatio=`echo "${tmp}+${failedReadsRatio}"| sed 's/E/*10^/g'`;	    
	fi

	tmp=`grep -a "timeoutRatioAbortedTransactions" ${scriptdir}/${client} | gawk -F':' '{print $2}'`;
	if [[ (! ${tmp} =~ '/') && (-n "${tmp}") ]]; then
	    timeoutRatio=`echo "${tmp}+${timeoutRatio}"| sed 's/E/*10^/g'`;	    
	fi

	tmp=`grep -a "transactionExecutionTime" ${scriptdir}/${client} | gawk -F':' '{print $2}'`;
	if [ -n "${tmp}" ]; then
	    executionTime=`echo "${tmp}+${executionTime}"| sed 's/E/*10^/g'`;	    
	fi

	tmp=`grep -a "transactionTerminationTime" ${scriptdir}/${client} | gawk -F':' '{print $2}'`;
	if [ -n "${tmp}" ]; then
	    terminationTime=`echo "${tmp}+${terminationTime}"| sed 's/E/*10^/g'`;	    
	fi

    done
    
    overallThroughput=`echo "scale=2;${overallThroughput}" | bc `;
    committedThroughput=`echo "scale=2;${committedThroughput}" | bc `;

    updateLatency=`echo "scale=2;(${updateLatency})/${#clients[@]}" | bc`;
    readLatency=`echo "scale=2;(${readLatency})/${#clients[@]}" | bc`;

    failedTerminationRatio=`echo "scale=5;(${failedTerminationRatio})/${#clients[@]}" | bc`;
    failedExecutionRatio=`echo "scale=5;(${failedExecutionRatio})/${#clients[@]}" | bc`;

    failedReadsRatio=`echo "scale=5;(${failedReadsRatio})/${#clients[@]}" | bc`;
    timeoutRatio=`echo "scale=5;(${timeoutRatio})/${#clients[@]}" | bc`;

    executionTime=`echo "scale=5;(${executionTime})/${#clients[@]}" | bc`;
    terminationTime=`echo "scale=5;(${terminationTime})/${#clients[@]}" | bc`;
    certificationTime=`echo "scale=5;(${certificationTime})/${#servers[@]}" | bc`;
}

function printServersMeasurements(){

echo "************** SERVER SIDE MEASUREMENTS  **************" >> $outputFilename;
echo "" >> $outputFilename;
echo "Overall_Throughput: " ${overallThroughput} >> $outputFilename;
echo "Committed_Throughput: " ${committedThroughput} >> $outputFilename;
echo "updateLatency: " ${updateLatency} >> $outputFilename;
echo "readLatency: " ${readLatency} >> $outputFilename;
echo "Failed_Termination_Ratio: " ${failedTerminationRatio} >> $outputFilename;
echo "Failed_Execution_Ratio: " ${failedExecutionRatio} >> $outputFilename;
echo "Failed_Read_Ratio: " ${failedReadsRatio} >> $outputFilename;
echo "Termination_Timeout_Ratio: " ${timeoutRatio} >> $outputFilename;
echo "Transaction_Execution_Time: " ${executionTime} >> $outputFilename;
echo "Transaction_Termination_Time: " ${terminationTime} >> $outputFilename;
echo "Certification_Time: " ${certificationTime} >> $outputFilename;
}

function printParameters(){

consistency=${cons[$selectedCons]}
servercount=${#servers[@]}
clientcount=`echo "${#clients[@]}*${t}" | bc`;

echo "************** CONFIGURATION PARAMETERS  **************" > $outputFilename;
echo "" >> $outputFilename;
echo "Consistency: " ${consistency} >> $outputFilename;
echo "Server_Machines: " ${servercount} >> $outputFilename;
echo "Client_Machines: " 	 $[${#clients[@]}] >> $outputFilename;
echo "Number_Of_Clients: " ${clientcount} >> $outputFilename;
echo "" >> $outputFilename;
}

echo "building measurements file..." 
collectMeasurementStrings &> stoutsterr
collectMeasurements &>> stoutsterr
printParameters &>> stoutsterr
printClientsMeasurements &>> stoutsterr
collectServersMeasurements &>> stoutsterr
printServersMeasurements
echo "done."

