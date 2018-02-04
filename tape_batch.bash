#!/bin/bash

TAPEOP='./tapeop'
LOGFILE=backup.log
op=$1

if [ "$op" == "" ]; then
    echo Valid commands are:
    echo '  ' archive confirm 
    exit 7
fi

case $op in
    archive)
	while true; do
	    date
	    $TAPEOP archive || break
	    echo
	    echo
	done
	;;

    confirm)
	while true; do
	    date
	    $TAPEOP status
	    echo
	    $TAPEOP confirm next || break
	    echo
	    echo
	done
	;;

    ltest)
	while true; do
	    date
	    sleep 1
	    echo
	done
	;;

    *)
	echo Unknown command "\"$op\"".
	;;
esac 2>&1 | tee -a $LOGFILE



