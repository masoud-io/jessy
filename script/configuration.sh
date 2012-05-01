#!/bin/sh


SSHCMD="oarsub -l nodes=1/core=2,walltime=2400:0:0" # ssh or oarsh for oar equiped cluster
scriptdir="/home/msaeida/jessy_script"
workingdir="/home/msaeida/jessy_exec"

#nodes=("aldebaran.rsr.lip6.fr" "cervin.rsr.lip6.fr" "ecrin.rsr.lip6.fr")
#nodes=("marek" "masoud")

nodes=("msaeida@cluster.lip6.fr")
servers=("cluster1u8")
clients=("cluster1u8")

# Experience settings

# Client specific settings 
workloadType="-load"
workloadName="transactionalworkloada"
nthreads=1
