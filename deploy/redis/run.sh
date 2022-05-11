#!/usr/bin/env bash

cd ../..

echo Option given = $@

java -jar /home/ben/project/drmbt/drmbt-code/build/libs/drmbt-code-all-1.0.jar -c /home/ben/project/drmbt/drmbt-code/deploy/redis/strata.config "$@"
