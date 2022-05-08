#!/bin/sh

#interval=60
interval=300
if [ ! -z "$1" ]; then
  interval=$1
fi

#./testrunner.sh &
./run.sh &
lastcheck=""
while [ true ]; do
  num1=`ls $2/record | wc -l`
  sleep $interval
  num2=`ls $2/record | wc -l`
  if [ $num1 -eq $num2 ]; then
    if [ "$lastcheck" = "1" ]; then
      touch lastcheckOneExit
      break
    fi
    lastcheck="1"
    #if [ ! $num1 -eq 0 ]; then
    #  remove=1
    #  for test_id in `find state -name .test_id`; do
    #    i=`cat $test_id`
    #    if [ $i -eq $num1 ]; then
    #      remove=""
    #      break
    #    fi
    #  done
    #  if [ "$remove" = "1" ]; then
    #    rm -r record/$num1
    #  fi
    #fi
    #killall java
    ./cleanenv.sh
    sleep 3
    rm -rf $2/record/$num1
    #./testrunner.sh &
    ./run.sh &
  else
    lastcheck=""
  fi
done
