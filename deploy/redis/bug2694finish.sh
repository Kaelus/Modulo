#!/bin/bash
A=8888
B=8889
C=8890
D=8891
BIN=~/project/redis/src/redis-server
CLI=~/project/redis/src/redis-cli

# Kill servers
$CLI -p $A SHUTDOWN NOSAVE
$CLI -p $B SHUTDOWN NOSAVE
$CLI -p $C SHUTDOWN NOSAVE
$CLI -p $D SHUTDOWN NOSAVE
