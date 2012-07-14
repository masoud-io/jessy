#!/bin/bash

source /home/msaeida/jessy_script/configuration.sh

function stopExp(){
    let sc=${#servers[@]}-1
    for j in `seq 0 $sc`
    do
	nohup ${SSHCMD} ${servers[$j]} "killall -SIGTERM java" 2&>1 > /dev/null &
    done

    sleep 5

    let e=${#nodes[@]}-1
    for i in `seq 0 $e`
    do
	echo "stopping on ${nodes[$i]}"
	nohup ${SSHCMD} ${nodes[$i]} "killall -9 java" 2&>1 > /dev/null &
    done

}

function dump(){
    let c=${#clients[@]}-1
    for j in `seq 0 $c`
    do
	echo "stopping on ${clients[$j]}"
	nohup ${SSHCMD} ${clients[$j]} "killall -SIGQUIT java \
			 		&& wait 5 \
					&& kilall -9 java" 2&>1 > /dev/null &
    done


}

function collectStats(){
    overallThroughput=0;
    committedThroughput=0;
    runtime=0;
    updateLatency=0
    readLatency=0
    consistency=${cons[$selectedCons]} #`grep 'consistency_type\ =' config.property | awk -F '=' '{print $2}'`
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
	    overallThroughput=`echo "${tmp} + ${overallThroughput}" | ${bc}`;
	fi

	runtime=`grep -a RunTime ${scriptdir}/${client} | gawk -F',' '{print $3}'`;
	tmp=`grep -a "Return=0" ${scriptdir}/${client} | awk -F "," '{sum+= $3} END {print sum}'`;
	if [ -n "${tmp}" ]; then
	    committedThroughput=`echo "((1000*${tmp})/${runtime}) + ${committedThroughput}" | ${bc}`;
	fi

	tmp=`grep -a "\[UPDATE\], AverageLatency" ${scriptdir}/${client} | gawk -F',' '{print $3}'`;
	if [ -n "${tmp}" ]; then
	    updateLatency=`echo "${tmp} + ${updateLatency}" | ${bc}`;
	fi

	tmp=`grep -a "\[READ\], AverageLatency" ${scriptdir}/${client} | gawk -F',' '{print $3}'`;
	if [ -n "${tmp}" ]; then
	    readLatency=`echo "${tmp} + ${readLatency}" | ${bc}`;
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
t=1;
    overallThroughput=`echo "scale=2;${overallThroughput}" | ${bc} `;
    committedThroughput=`echo "scale=2;${committedThroughput}" | ${bc} `;

    updateLatency=`echo "scale=2;(${updateLatency})/${#clients[@]}" | ${bc}`;
    readLatency=`echo "scale=2;(${readLatency})/${#clients[@]}" | ${bc}`;
    clientcount=`echo "${#clients[@]}*${t}" | ${bc}`;

    failedTerminationRatio=`echo "scale=10;(${failedTerminationRatio})/${#clients[@]}" | ${bc}`;
    failedExecutionRatio=`echo "scale=10;(${failedExecutionRatio})/${#clients[@]}" | ${bc}`;

    failedReadsRatio=`echo "scale=10;(${failedReadsRatio})/${#clients[@]}" | ${bc}`;
    timeoutRatio=`echo "scale=10;(${timeoutRatio})/${#clients[@]}" | ${bc}`;

    executionTime=`echo "scale=10;(${executionTime})/${#clients[@]}" | ${bc}`;
    terminationTime=`echo "scale=10;(${terminationTime})/${#clients[@]}" | ${bc}`;
    certificationTime=`echo "scale=10;(${certificationTime})/${#servers[@]}" | ${bc}`;

    
    echo -e  "${consistency}\t${servercount}\t$[${#clients[@]}]\t${clientcount}\t${overallThroughput}\t${committedThroughput}\t${updateLatency}\t${readLatency}\t${failedTerminationRatio}\t${failedExecutionRatio}\t${failedReadsRatio}\t${timeoutRatio}\t${executionTime}\t${terminationTime}\t${certificationTime}"

}

trap "stopExp; wait; exit 255" SIGINT SIGTERM
trap "dump; wait;" SIGQUIT


# ##############
# # Experience #
# ##############
let servercount=${#servers[@]}

let consCount=${#cons[@]}-1
for selectedCons in `seq 0 $consCount`
do  

	sed -i "s/consistency_type.*/consistency_type\ =\ ${cons[$selectedCons]}/g" config.property

	#thread setup
	thread=`seq ${client_thread_glb} ${client_thread_increment} ${client_thread_lub}`


	for t in ${thread}; 
	do

	# 0 - Starting the server

	    echo "Starting servers ..."
	    sed -i 's/-t/-load/g' configuration.sh
	    ${scriptdir}/launcher.sh &


	# 1 - Loading phase
	    echo "Loading phase ..."
	    ${SSHCMD} ${clients[0]} "${scriptdir}/client.sh" > ${scriptdir}/loading

	    sleep 20

	# 2 - Benchmarking phase

	    echo "Benchmarking phase ..."
	    sed -i 's/-load/-t/g' configuration.sh

	    sed -i "s/nthreads.*/nthreads=${t}/g" configuration.sh
	    echo "using ${t} thread(s) per machine"   
	    ${scriptdir}/clauncher.sh

	    stopExp

	    echo "using ${t} thread(s) per machine is finished. Collecting stats"   
	    collectStats >>  ${scriptdir}/results/${servercount}.txt

	    sleep 10
	    
	done

done
