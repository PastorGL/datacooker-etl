#!/usr/bin/env bash

#NB! Never automate that action

mvn com.mycila:license-maven-plugin:format -Dlicense.header=./commons/license.inc -pl datacooker-etl-cli -pl datacooker-commons -pl datacooker-datetime -pl datacooker-geohashing -pl datacooker-math -pl datacooker-populations -pl datacooker-proximity -pl datacooker-spatial
