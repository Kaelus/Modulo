#!/usr/bin/env bash

cd ../..

echo Option given = $@

java -jar build/libs/modulo-all-1.0.jar -c deploy/zookeeper/strata.config "$@"
