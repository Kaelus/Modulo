#!/usr/bin/env bash

if [ $# -lt 2 ]; then
    echo "usage: setup.sh <num_node> <working_dir>"
    exit 1
fi

num_node=$1
working_dir=$2
conf_dir=$working_dir/conf

scriptdir=`dirname $0`

i=0
while [ $i -lt $num_node ]; do
    test -d $working_dir/redis_dir/$i && rm -rf $working_dir/redis_dir/$i
    mkdir -p $working_dir/redis_dir/$i
    cp -rf $scriptdir/redis.conf-template $working_dir/redis_dir/$i/redis.conf
    #cp -rf $scriptdir/sentinel.conf-template $working_dir/redis_dir/$i/sentinel.conf

    # modify redis.conf
    portNumber=`expr 6379 + $i`
    sed -i -e "92s|.*|port $portNumber|" $working_dir/redis_dir/$i/redis.conf

    sed -i -e "158s|.*|pidfile $working_dir/redis_dir/$i/redis.pid|" $working_dir/redis_dir/$i/redis.conf

    sed -i -e "263s|.*|dir $working_dir/redis_dir/$i/|" $working_dir/redis_dir/$i/redis.conf

    sed -i -e "265s|.*|logfile \"redis-server.log\"|" $working_dir/redis_dir/$i/redis.conf
    
    #if [ $i -ne 0 ]; then
	#sed -i -e "281s|.*|slaveof 127.0.0.1 6379|" $working_dir/redis_dir/$i/redis.conf
    #fi
    
    # modify sentinel.conf
    # sentinelPortNumber=`expr 26379 + $i`
    # sed -i -e "21s|.*|port $sentinelPortNumber|" $working_dir/redis_dir/$i/sentinel.conf
    
    # sed -i -e "50s|.*|dir $working_dir/redis_dir/$i|" $working_dir/redis_dir/$i/sentinel.conf

    # echo "logfile \"$working_dir/redis_dir/$i/redis-sentinel.log\"" >> $working_dir/redis_dir/$i/sentinel.conf
    
    i=`expr $i + 1`
done

