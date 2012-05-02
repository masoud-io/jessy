#!/bin/sh


SSHCMD="oarsh" # ssh or oarsh for oar equiped cluster
scriptdir="/home/msaeida/jessy_script"
workingdir="/tmp/jessy_exec"

#nodes=("aldebaran.rsr.lip6.fr" "cervin.rsr.lip6.fr" "ecrin.rsr.lip6.fr")
#nodes=("marek" "masoud")

nodes=("cluster1u7" "cluster1u8" "cluster1u9")
servers=("cluster1u7")
clients=("cluster1u8")

# Experience settings

# Client specific settings 
workloadType="-load"
workloadName="transactionalworkloada"
nthreads=1
