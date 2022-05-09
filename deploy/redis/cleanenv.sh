#!/usr/bin/env bash

kill -9 $(ps aux | grep 'redis-server ' | grep -v grep | awk '{print $2}')
kill -9 $(ps aux | grep 'redis-sentinel ' | grep -v grep | awk '{print $2}')
kill $(ps aux | grep 'drmbt-code-all-1.0.jar' | grep -v grep | awk '{print $2}')

