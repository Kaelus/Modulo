#!/bin/sh

# $1 should be where redis-server is
# and $2 should be where configuration file is
# e.g., /home/ben/project/drmbt/drmbt-code/sut/redis-server /home/ben/experiment/test-2-4-Redis-2.8.0-strata-0.1/redis_dir/0/redis.conf
echo 'Executing: ' $1/redis-server $2
$1/redis-server $2 &


