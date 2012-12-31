#!/bin/sh

	echo "Killing clauncher"
	procno=(`ps aux | grep clauncher | grep bin | awk 'BEGIN{ORS=" ";} {print $2}'`)

	kill -10 ${procno[0]}
	kill -9 ${procno[1]}
