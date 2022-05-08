#!/bin/sh

interval=360
if [ ! -z "$1" ]; then
    interval=$1
fi

./run.sh &
while [ true ]; do
    num1=`ls $2/record | wc -l`  
    sleep $interval
    num2=`ls $2/record | wc -l`
    ./cleanenv.sh
    sleep 5
    if [ $num1 -eq $num2 ]; then
	if [ "$lastcheck" = "1" ]; then
	    break
	fi
	lastcheck="1"
    else
	lastcheck=""
    fi
    rm -rf $2/record/$num2
    ./run.sh &
    sleep 5
done
