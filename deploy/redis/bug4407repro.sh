#!/bin/bash

# Script to reproduce redis bug 4416

# -1. Environment setup
echo "-1. Environment setup"
#WORKING_DIR=/home/ben/experiment/test-3-1-Redis-4.0.0-strata-0.1
WORKING_DIR=/home/ben/experiment/redis_performance_measure/4407/test-1-3-Redis-4.0.0-strata-0.1
DRMBT_SCRIPT_DIR=/home/ben/project/drmbt/drmbt-code/deploy/redis
#DRMBT_SUT_DIR=/home/ben/project/drmbt/drmbt-code/sut
DRMBT_SUT_DIR=/home/ben/project/redis/src
A=6379
B=6380
C=6381
CLI=/home/ben/project/redis/src/redis-cli
DELAY=5
cd $WORKING_DIR
$DRMBT_SCRIPT_DIR/setup.sh 3 $WORKING_DIR
echo 'WORKING_DIR=' $WORKING_DIR
echo 'DRMBT_SCRIPT_DIR=' $DRMBT_SCRIPT_DIR
echo 'DRMBT_SUT_DIR=' $DRMBT_SUT_DIR
echo 'port A is' $A
echo 'port B is' $B
echo 'port C is' $C
echo 'delay we inject is' $DELAY 'sec'
echo 'current directory should be working directory. cur dir=' `pwd`
echo 'creating redis_dir in working directory is done'
ls -R $WORKING_DIR/redis_dir

# 0. INIT
echo "0. INIT"
$DRMBT_SCRIPT_DIR/start-redis.sh $DRMBT_SUT_DIR $WORKING_DIR/redis_dir/0/redis.conf &
$DRMBT_SCRIPT_DIR/start-redis.sh $DRMBT_SUT_DIR $WORKING_DIR/redis_dir/1/redis.conf &
$DRMBT_SCRIPT_DIR/start-redis.sh $DRMBT_SUT_DIR $WORKING_DIR/redis_dir/2/redis.conf &
sleep 1
$CLI -p $C SLAVEOF 127.0.0.1 $B
$CLI -p $B SLAVEOF 127.0.0.1 $A
$CLI -p $A set foo bar
sleep 1

# 1. DIV [1, 0, 0]
$CLI -p $B SLAVEOF NO ONE
sleep $DELAY
sleep 3
$CLI -p $A set foo1 bar1
sleep 1

# 2. RES [0] 0<-1
$CLI -p $A SLAVEOF 127.0.0.1 $B
sleep 1

# 00. VERIF
echo "00. VERIF"
sleep 10
$CLI -p $A get foo
$CLI -p $B get foo
$CLI -p $C get foo
$CLI -p $A get foo1
$CLI -p $B get foo1
$CLI -p $C get foo1

# Kill servers
echo "Finishing..."
$CLI -p $A SHUTDOWN NOSAVE
$CLI -p $B SHUTDOWN NOSAVE
$CLI -p $C SHUTDOWN NOSAVE
