#!/usr/bin/env bash

cd ../..

echo Option given = $@

java -jar /home/modulo/modulo/build/libs/modulo-all-1.0.jar -c /home/modulo/modulo/deploy/redis/strata.config "$@"
