#!/usr/bin/env bash

kill -9 $(ps aux | grep 'mongod ' | grep -v grep | awk '{print $2}')
kill $(ps aux | grep 'modulo-code-all-1.0.jar' | grep -v grep | awk '{print $2}')

