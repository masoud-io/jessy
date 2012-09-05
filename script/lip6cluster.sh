#!/bin/sh

source /home/msaeida/jessy_script/configuration.sh

trap "kill 0; wait; exit 255" SIGINT SIGTERM

# 2 - Configuring
nvar=(`oarstat -f | grep assigned_hostnames | gawk -F= '{print $2}' | sed s/+/" "/g`)

# 3 - Launching
nservers=`seq ${server_machine_glb} ${server_machine_increment} ${server_machine_lub}`
#nclients=`seq ${client_machine_glb} ${client_machine_increment} ${client_machine_lub}`
for s in ${nservers}; 
do

	client_machine_lub=`echo "scale=1;${s}*${client_machine_lub_multiplier}" | ${bc}`;
	nclients=`seq ${s} ${client_machine_increment} ${client_machine_lub}`
    # Construct servers variable.
    svar="\"${nvar[0]}\""
    for i in `seq 1 $[${s}-1]`
    do
	svar="${svar} \"${nvar[$i]}\""
    done
    svar="servers=(${svar})"
    sed -i "s/servers=.*/${svar}/g" configuration.sh

    # Generate the fractal XML file
    echo '<?xml version="1.0" encoding="ISO-8859-1" ?>' > tmp.xml
    echo '<FRACTAL>' >> tmp.xml
    echo ' <BootstrapIdentity>' >> tmp.xml
    echo '  <nodelist>' >> tmp.xml
    for i in `seq 0 $[${s}-1]`
    do
	ip=`host ${nvar[$i]} | gawk -F" " '{print $4}'`
	echo "   <node id=\"${i}\" ip=\"${ip}\"/>" >> tmp.xml
    done
    echo "  </nodelist>" >> tmp.xml
    echo " </BootstrapIdentity>" >> tmp.xml
    echo "</FRACTAL>" >> tmp.xml
    mv tmp.xml myfractal.xml

    for c in ${nclients};
    do

        # Construct clients variable.
	let a=$[${i}+1]
	cvar="\"${nvar[$a]}\""
	for j in `seq $[${i}+2] $[${i}+${c}]`
 	do
 	    cvar="${cvar} \"${nvar[$j]}\""
	done
	cvar="clients=(${cvar})"
        sed -i "s/clients=.*/${cvar}/g" configuration.sh

	# Now, launching
	echo "Launching with ${s} server(s) and ${c} client(s)"
	${scriptdir}/experience.sh       
	
   done
    
done

