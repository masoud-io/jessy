#!/bin/bash

#cluster to be used:
clusters=("grenoble" "lille" "bordeaux")
#true if the deploy will affect the first cluster, then the first and the second etc. False if the deploy is on all clusters only. Each cluster will hospitate the same amount of servers and clients
varyClusterDeployment=false
#avoid to run with a different number of servers in different clusters. This can happen, for example when 4 or 5 servers are unsed over 3 clusters
avoidUnbalancedRuns=false

minServers=1
maxServers=3
serverIncrement=1

minClientsForEachServer=2
maxClientsForEachServer=2
clientIncrement=1

#WARNING this override client_thread_increment, client_thread_glb and client_thread_lub in configuration.sh
clientsThreadIncrement="1"
minClientsThread="2"
maxClientsThread="2"

# WARNING this override cons in configuration.sh
consistency=("nmsi2") # "us" "psi") # "nmsi2" "us" "ser")
