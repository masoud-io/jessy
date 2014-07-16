#!/bin/bash
# prints the jessy client outputs to the screen.
# Note, this script should be run in the main frontend! not in the sites!
#

source /home/msaeidaardekani/lille/jessy/scripts/configuration.sh

cmd=""
tmpClient=""
tmpSite=""
let cc=${#clients[@]}-1
for ii in `seq 0 $cc`;
do
	tmpClient=${clients[${ii}]}

	tmpSite=`echo ${tmpClient} | awk -F. '{print $2}'`

        cmd=$cmd" /home/msaeidaardekani/"${tmpSite}"/jessy/scripts/"${tmpClient}".sterr"
done
tail -f $cmd
