#!/usr/bin/env bash

cd ../..

echo Option given = $@

java -jar `pwd`/../../build/libs/modulo-all-1.0.jar -c `pwd`/../../deploy/zookeeper/strata.config "$@"
