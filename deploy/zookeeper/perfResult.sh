#!/usr/bin/env bash

if [ $# -lt 1 ] || [ $# -gt 2 ]; then
    echo "usage: perfResult.sh <working_dir> [<record_number>]"
    exit 1
fi

cd $1

if [ $# -eq 1 ]; then
    echo "-----------------------------------------------------"
    echo "Getting the stat for all records"    
    echo "-----------------------------------------------------"
    cumDuration=`cat record/*/performance.stat | cut -d '=' -f 2 | python -c "import sys; print(sum(int(l) for l in sys.stdin))"`
    echo "Schedule Executions took in total=$cumDuration"
    scheduleCount=`ls record/*/performance.stat | wc -l`
    echo "There have been schedule executions as many as $scheduleCount" 
    avgDuration=`expr $cumDuration / $scheduleCount`
    echo "Average schedule execution time=$avgDuration"
    echo "-----------------------------------------------------"
elif [ $# -eq 2 ]; then
    recordNumber=$2
    echo "-----------------------------------------------------"
    echo "Getting the stat for records as many as $recordNumber"
    echo "-----------------------------------------------------"
    files=`ls record | sort -n | sed -n 1,${recordNumber}p`
    cumDuration=0
    for file in $files;
    do
	perStat=`cat record/$file/performance.stat | cut -d '=' -f 2`
	cumDuration=`expr $cumDuration + $perStat`
    done
        echo "Schedule Executions took in total=$cumDuration"
    scheduleCount=$recordNumber
    echo "There have been schedule executions as many as $scheduleCount" 
    avgDuration=`expr $cumDuration / $scheduleCount`
    echo "Average schedule execution time=$avgDuration"
    echo "-----------------------------------------------------"
else
    echo "Unknown Error"
    exit 1
fi
