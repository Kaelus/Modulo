#!/bin/bash

A=8888
B=8889
C=8890
D=8891
#BIN=~/project/redis/src/redis-server
BIN=~/project/drmbt/drmbt-code/sut/redis-server
CLI=~/project/redis/src/redis-cli

# 0. INIT
echo "0. INIT"
mkdir -p /tmp/node0; rm -rf /tmp/node0/*
mkdir -p /tmp/node1; rm -rf /tmp/node1/*
mkdir -p /tmp/node2; rm -rf /tmp/node2/*
mkdir -p /tmp/node3; rm -rf /tmp/node3/*

$BIN --logfile /tmp/node0/redis.log --port $A &
$BIN --logfile /tmp/node1/redis.log --port $B &
$BIN --logfile /tmp/node2/redis.log --port $C &
$BIN --logfile /tmp/node3/redis.log --port $D &

sleep 2
$CLI -p $A SLAVEOF NO ONE
$CLI -p $B SLAVEOF NO ONE
$CLI -p $C SLAVEOF NO ONE
$CLI -p $D SLAVEOF NO ONE

$CLI -p $A FLUSHALL
$CLI -p $B FLUSHALL
$CLI -p $C FLUSHALL
$CLI -p $D FLUSHALL

ps aux | grep redis

$CLI -p $A set a 0
$CLI -p $B set a 0
$CLI -p $C set a 0
$CLI -p $D set a 0
sleep 2
$CLI -p $A get a
$CLI -p $B get a
$CLI -p $C get a
$CLI -p $D get a

# 1. DIV [1,0,0,0]
echo "1. DIV [1,0,0,0]"
kill -STOP $(ps aux | grep redis | grep $B | awk '{print $2}')
kill -STOP $(ps aux | grep redis | grep $C | awk '{print $2}')
kill -STOP $(ps aux | grep redis | grep $D | awk '{print $2}')
ps aux | grep redis
$CLI -p $A set a 1
ps aux | grep redis
sleep 2
kill -STOP $(ps aux | grep redis | grep $A | awk '{print $2}')
sleep 2

# 2. RES [1,2] B->C
echo "2. RES [1,2] B->C"
kill -CONT $(ps aux | grep redis | grep $B | awk '{print $2}')
kill -CONT $(ps aux | grep redis | grep $C | awk '{print $2}')
$CLI -p $C SLAVEOF 127.0.0.1 $B
sleep 2

# 3. RES [3] C->D
echo "3. RES [3] C->D"
kill -CONT $(ps aux | grep redis | grep $C | awk '{print $2}')
kill -CONT $(ps aux | grep redis | grep $D | awk '{print $2}')
$CLI -p $D SLAVEOF 127.0.0.1 $C
sleep 2

# 4. DIV [0,1,1,1]
echo "4. DIV [0,1,1,1]"
kill -STOP $(ps aux | grep redis | grep $A | awk '{print $2}')
$CLI -p $B set a 2
sleep 2
#$CLI -p $B SLAVEOF NO ONE
#$CLI -p $C SLAVEOF NO ONE
#$CLI -p $D SLAVEOF NO ONE
kill -STOP $(ps aux | grep redis | grep $B | awk '{print $2}')
kill -STOP $(ps aux | grep redis | grep $C | awk '{print $2}')
kill -STOP $(ps aux | grep redis | grep $D | awk '{print $2}')
sleep 2

# 5. RES [1,2] B->C
echo "5. RES [1,2] B->C"
kill -CONT $(ps aux | grep redis | grep $B | awk '{print $2}')
kill -CONT $(ps aux | grep redis | grep $C | awk '{print $2}')
#$CLI -p $C SLAVEOF 127.0.0.1 $B
sleep 2

# 6. RES [0] A->B
echo "6. RES [0] A->B"
kill -CONT $(ps aux | grep redis | grep $A | awk '{print $2}')
#kill -CONT $(ps aux | grep redis | grep $B | awk '{print $2}')
$CLI -p $B SLAVEOF 127.0.0.1 $A
sleep 2

# 7. RES [3] C->D
echo "7. RES [3] C->D"
#kill -CONT $(ps aux | grep redis | grep $C | awk '{print $2}')
kill -CONT $(ps aux | grep redis | grep $D | awk '{print $2}')
#$CLI -p $D SLAVEOF 127.0.0.1 $C
sleep 2

# 00. VERIF
echo "00. VERIF"
sleep 10
$CLI -p $A get a
$CLI -p $B get a
$CLI -p $C get a
$CLI -p $D get a

# Kill servers
echo "Finishing..."
$CLI -p $A SHUTDOWN NOSAVE
$CLI -p $B SHUTDOWN NOSAVE
$CLI -p $C SHUTDOWN NOSAVE
$CLI -p $D SHUTDOWN NOSAVE
