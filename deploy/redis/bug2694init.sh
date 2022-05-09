#!/bin/bash

A=8888
B=8889
C=8890
D=8891
BIN=~/project/redis/src/redis-server
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
CLI=~/project/redis/src/redis-cli
$CLI -p $A SLAVEOF NO ONE
$CLI -p $B SLAVEOF NO ONE
$CLI -p $C SLAVEOF NO ONE
$CLI -p $D SLAVEOF NO ONE

$CLI -p $A FLUSHALL
$CLI -p $B FLUSHALL
$CLI -p $C FLUSHALL
$CLI -p $D FLUSHALL
