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
    cp -rf $scriptdir/mongod.conf-template $conf_dir/$i/mongod.conf

    test -d $working_dir/log/$i && rm -rf $working_dir/log/$i
    mkdir -p $working_dir/log/$i
    systemLog_path=$working_dir/log/$i/mongod.log
    sed -i -e "11s|.*|  path: $systemLog_path|" $conf_dir/$i/mongod.conf

    test -d $working_dir/data/$i && rm -rf $working_dir/data/$i
    mkdir -p $working_dir/data/$i
    storage_dbPath=$working_dir/data/$i
    sed -i -e "15s|.*|  dbPath: $storage_dbPath|" $conf_dir/$i/mongod.conf

    processManagement_pidFilePath=$working_dir/data/$i/mongod.pid
    sed -i -e "25s|.*|  pidFilePath: $processManagement_pidFilePath|" $conf_dir/$i/mongod.conf

    portNumber=`expr 27017 + $i`
    sed -i -e "30s|.*|  port: $portNumber|" $conf_dir/$i/mongod.conf

    echo replication: >> $conf_dir/$i/mongod.conf
    echo "  replSetName: repl1" >> $conf_dir/$i/mongod.conf

    i=`expr $i + 1`
done

