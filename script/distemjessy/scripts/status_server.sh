#!/bin/bash
# prints the jessy server outputs to the screen.
# Note, this script should be run in the main frontend! not in the sites!
#

source /home/msaeidaardekani/nancy/jessy/scripts/configuration.sh

cmd=""
tmpServer=""
tmpSite=""
let cc=${#servers[@]}-1
for ii in `seq 0 $cc`;
do
	tmpServer=${servers[${ii}]}

	tmpSite=`echo ${tmpServer} | awk -F. '{print $2}'`

        cmd=$cmd" /home/msaeidaardekani/"${tmpSite}"/jessy/scripts/"${tmpServer}
done
tail -f $cmd
