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
    test -d $conf_dir/$i && rm -rf $conf_dir/$i
    mkdir -p $conf_dir/$i
    cp -rf $scriptdir/zoo.cfg-template $conf_dir/$i/zoo.cfg
    cp -rf $scriptdir/zk_log.properties $conf_dir/$i/zk_log.properties

    test -d $working_dir/log/$i && rm -rf $working_dir/log/$i
    mkdir -p $working_dir/log/$i
    
    test -d $working_dir/data/$i && rm -rf $working_dir/data/$i
    mkdir -p $working_dir/data/$i
    #storage_dbPath=$working_dir/data/$i
    #sed -i -e "15s|.*|  dbPath: $storage_dbPath|" $conf_dir/$i/mongod.conf
    sed -i -e "12s|.*|dataDir=$working_dir/data/$i|" $conf_dir/$i/zoo.cfg

    #processManagement_pidFilePath=$working_dir/data/$i/mongod.pid
    #sed -i -e "25s|.*|  pidFilePath: $processManagement_pidFilePath|" $conf_dir/$i/mongod.conf
    echo "$i" > $working_dir/data/$i/myid
    
    #portNumber=`expr 27017 + $i`
    #sed -i -e "30s|.*|  port: $portNumber|" $conf_dir/$i/mongod.conf
    cliPort=`expr 12180 + $i`
    sed -i -e "14s|.*|clientPort=$cliPort|" $conf_dir/$i/zoo.cfg
    
    #echo replication: >> $conf_dir/$i/mongod.conf
    #echo "  replSetName: repl1" >> $conf_dir/$i/mongod.conf
    echo "" >> $conf_dir/$i/zoo.cfg
    j=0
    while [ $j -lt $num_node ]; do
	srvPort=`expr 12890 + $j`
	srvLEPort=`expr 13880 + $j`
	echo "server.$j=localhost:$srvPort:$srvLEPort" >> $conf_dir/$i/zoo.cfg
	j=`expr $j + 1`
    done

    i=`expr $i + 1`
done

