#!/bin/bash

# Script to reproduce redis bug 4316 case 1
# RAW sequence (Requirement: client for node 0 should never be disconnected
# for stateful connection)
#   start node 0
#   start node 1
#   1 slaveof 0
#   set foo bar on 0
#   kill 1
#   select 1 on 0
#   set foo1 bar1 on 0
#   bgsave on 0
#   offline sync from 0 to 1
#   start 1
#   set foo2 bar2
#   [verify]
#
# NOTE: The test sequence below is not necessarily exact copy of the raw
# sequence below
#

# -1. Environment setup
echo "-1. Environment setup"
WORKING_DIR=/home/ben/experiment/test-2-1-Redis-4.0.0-strata-0.1
DRMBT_SCRIPT_DIR=/home/ben/project/drmbt/drmbt-code/deploy/redis
DRMBT_SUT_DIR=/home/ben/project/drmbt/drmbt-code/sut
A=6379
B=6380
CLI=/home/ben/project/redis/src/redis-cli
cd $WORKING_DIR
$DRMBT_SCRIPT_DIR/setup.sh 2 $WORKING_DIR
echo 'WORKING_DIR=' $WORKING_DIR
echo 'DRMBT_SCRIPT_DIR=' $DRMBT_SCRIPT_DIR
echo 'DRMBT_SUT_DIR=' $DRMBT_SUT_DIR
echo 'port A is' $A
echo 'port B is' $B
echo 'current directory should be working directory. cur dir=' `pwd`
echo 'creating redis_dir in working directory is done'
ls -R $WORKING_DIR/redis_dir

# 0. INIT
echo "0. INIT"
$DRMBT_SCRIPT_DIR/start-redis.sh $DRMBT_SUT_DIR $WORKING_DIR/redis_dir/0/redis.conf &
$DRMBT_SCRIPT_DIR/start-redis.sh $DRMBT_SUT_DIR $WORKING_DIR/redis_dir/1/redis.conf &
sleep 1

$CLI -p $B SLAVEOF 127.0.0.1 6379
sleep 1
$CLI -p $A set foo bar
sleep 1

# 1. DIV [1, 0]
kill -9 $(ps aux | grep redis | grep $B | awk '{print $2}')
#$CLI -p $A SELECT 1
#$CLI -p $A set foo1 bar1
#(echo -e "SELECT 1\nset foo1 bar1\ndebug sleep 1\nBGSAVE\ndebug sleep 5\nset foo2 bar2" | $CLI -p $A) &
(echo -e "SELECT 1\nset foo bar1\ndebug sleep 1\nBGSAVE\ndebug sleep 5\nset foo bar2" | $CLI -p $A) &
sleep 2

# 2. RES [0, 1] 0->1 OFFSYNC
#$CLI -p $A BGSAVE
cp $WORKING_DIR/redis_dir/0/dump.rdb $WORKING_DIR/redis_dir/1/.
$DRMBT_SCRIPT_DIR/start-redis.sh $DRMBT_SUT_DIR $WORKING_DIR/redis_dir/1/redis.conf &
sleep 1

# 3. RES [0, 1] 0->1 ONSYNC
$CLI -p $B SLAVEOF 127.0.0.1 6379
sleep 1

# 00. VERIF
echo "00. VERIF"
#$CLI -p $A set foo3 bar3
#echo -e "SELECT 1\nset foo2 bar2" | $CLI -p $A
sleep 10

#$CLI -p $A SELECT 0
#$CLI -p $B SELECT 0
#$CLI -p $A get foo
#$CLI -p $B get foo
#$CLI -p $A get foo1
#$CLI -p $B get foo1
#$CLI -p $A get foo2
#$CLI -p $B get foo2
echo -e "SELECT 0\nget foo\nget foo1\nget foo2" | $CLI -p $A
echo -e "SELECT 0\nget foo\nget foo1\nget foo2" | $CLI -p $B
echo -e "SELECT 1\nget foo\nget foo1\nget foo2" | $CLI -p $A
echo -e "SELECT 1\nget foo\nget foo1\nget foo2" | $CLI -p $B

# Kill servers
echo "Finishing..."
$CLI -p $A SHUTDOWN NOSAVE
$CLI -p $B SHUTDOWN NOSAVE

