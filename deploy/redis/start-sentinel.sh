#!/bin/sh

echo 'Executing: ' $1/redis-sentinel $2
$1/redis-sentinel $2

