#!/bin/bash

A=8888
B=8889
C=8890
D=8891
BIN=~/project/redis/src/redis-server
CLI=~/project/redis/src/redis-cli

# 00. VERIF
$CLI -p $A get a
$CLI -p $B get a
$CLI -p $C get a
$CLI -p $D get a
