#!/bin/sh

#clauncher.sh timeout. Within this timeout, if the clauncher is not finished, it will be terminated and re-executed.
#this is needed because jessy clients sometimes never finish.
clauncherTimeout=600

#if it will be run on grid5k, set to true, otherwise, set to false.
#running_on_grid=true

SSHCMD="ssh" # ssh or oarsh for oar equiped cluster

#basic calculator path. Note: lip6 cluster does not have bc.
bc="./bc"

#The location of all the scripts.
scriptdir=/root/distemjessy/scripts

#The location where all the databases will be created.
workingdir="/tmp/jessy_msaeida"

#Read by oarlauncher for reserving the nodes. 
nodes=( "chinqchint-34.lille.grid5000.fr" "chinqchint-35.lille.grid5000.fr" "chinqchint-37.lille.grid5000.fr" "chinqchint-39.lille.grid5000.fr" "edel-25.grenoble.grid5000.fr" "edel-56.grenoble.grid5000.fr" "edel-57.grenoble.grid5000.fr" "edel-58.grenoble.grid5000.fr" "griffon-68.nancy.grid5000.fr" "griffon-70.nancy.grid5000.fr" "griffon-71.nancy.grid5000.fr" "griffon-78.nancy.grid5000.fr" "paradent-53.rennes.grid5000.fr" "paradent-54.rennes.grid5000.fr" "paradent-55.rennes.grid5000.fr" "paradent-58.rennes.grid5000.fr")
servers=( "chinqchint-34.lille.grid5000.fr" "chinqchint-35.lille.grid5000.fr" "edel-25.grenoble.grid5000.fr" "edel-56.grenoble.grid5000.fr" "griffon-68.nancy.grid5000.fr" "griffon-70.nancy.grid5000.fr" "paradent-53.rennes.grid5000.fr" "paradent-54.rennes.grid5000.fr")
clients=( "chinqchint-37.lille.grid5000.fr" "chinqchint-39.lille.grid5000.fr" "edel-57.grenoble.grid5000.fr" "edel-58.grenoble.grid5000.fr" "griffon-71.nancy.grid5000.fr" "griffon-78.nancy.grid5000.fr" "paradent-55.rennes.grid5000.fr" "paradent-58.rennes.grid5000.fr")

# Experience.sh settings

# servers
server_machine_increment="20"
server_machine_glb="4"
server_machine_lub="4"

# clients ( = client_machine * client_thread )
client_machine_increment="1"

#client_machine_glb = client_machine_lub_multiplier * number of servers
client_machine_glb_multiplier="1" 

#client_machine_lub = client_machine_lub_multiplier * number of servers
client_machine_lub_multiplier="1" 

client_thread_increment=(100)
client_thread=(100 100)

# Consistency
cons=("rc") #("rc" "nmsi_pdv_2pc" "walter_vv_2pc" "gmu_gmv_2pc") #("rc" "nmsi_pdv_2pc" "walter_vv_2pc") # "gmu_gmv_2pc") # ("walter_vv_2pc" "gmu_gmv_2pc" "rc") #("pstore_lsv_gc" "ser_pdv_gc")

# Client specific settings 
workloadType="-t"
workloadName="transactionalworkloada"
nthreads=6000

system=jessy

if [[ ${system} == "cassandra"  ]];
then
    clientclass=com.yahoo.ycsb.CassandraClient10;
    classpath=${scriptdir}/jessy.jar
    for jar in ${scriptdir}/cassandra/lib/*.jar; do
	classpath=$classpath:$jar
    done
fi;

if [[ ${system} == "jessy"  ]];
then
    clientclass=com.yahoo.ycsb.JessyDBClient;
    classpath=${scriptdir}/commons-lang.jar:${scriptdir}/log4j.jar:${scriptdir}/jessy.jar:${scriptdir}/fractal.jar:${scriptdir}/je.jar:${scriptdir}/concurrentlinkedhashmap.jar:${scriptdir}/netty.jar:${scriptdir}/high-scale-lib.jar:${scriptdir}/db.jar;
fi;
