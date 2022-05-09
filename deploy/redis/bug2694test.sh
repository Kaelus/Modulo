#!/bin/bash
mkdir -p /tmp/a; rm -rf /tmp/a/*
mkdir -p /tmp/b; rm -rf /tmp/b/*
mkdir -p /tmp/c; rm -rf /tmp/c/*
mkdir -p /tmp/d; rm -rf /tmp/d/*
A=8888
B=8889
C=8810
D=8811
BIN=~/project/redis/src/redis-server
$BIN --logfile /tmp/a/redis.log --port $A &
$BIN --logfile /tmp/b/redis.log --port $B &
$BIN --logfile /tmp/c/redis.log --port $C &
$BIN --logfile /tmp/d/redis.log --port $D &

sleep 2
redis-cli -p $A SLAVEOF NO ONE
redis-cli -p $B SLAVEOF NO ONE
redis-cli -p $C SLAVEOF NO ONE
redis-cli -p $D SLAVEOF NO ONE

redis-cli -p $A FLUSHALL
redis-cli -p $B FLUSHALL
redis-cli -p $C FLUSHALL
redis-cli -p $D FLUSHALL

# Setup A, B <- C <- D
redis-cli -p $C SLAVEOF 127.0.0.1 $B
redis-cli -p $D SLAVEOF 127.0.0.1 $C

# Write the two keys
redis-cli -p $A set a 1
redis-cli -p $B set a 2
sleep 2

echo "HERE 1"

# Setup the SLEEP & RECONNECT condition for D
redis-cli -p $D client list
(echo -e "multi\nclient kill id 5\ndebug sleep 5\nexec\n" | redis-cli -p $D) &

echo "HERE 2"

# Make B slave of A
sleep 1
redis-cli -p $B SLAVEOF 127.0.0.1 $A

echo "HERE 3"

redis-cli -p $A ping
redis-cli -p $B ping
redis-cli -p $C ping
echo "HERE 4"
redis-cli -p $D ping

echo "HERE 5"
echo "get a from A"
redis-cli -p $A get a
echo "get a from B"
redis-cli -p $B get a
echo "get a from C"
redis-cli -p $C get a
echo "get a from D"
redis-cli -p $D get a

# Fetch the value
sleep 6
echo "The following value should be 1 but is 2 because of the bug:"
redis-cli -p $D get a

# Kill servers
redis-cli -p $A SHUTDOWN NOSAVE
redis-cli -p $B SHUTDOWN NOSAVE
redis-cli -p $C SHUTDOWN NOSAVE
redis-cli -p $D SHUTDOWN NOSAVE
