#!/usr/bin/env bash

#NB! Never automate that action

mvn com.mycila:license-maven-plugin:format -Dlicense.header=./commons/license.inc -pl datacooker-dist-cli -pl datacooker-jdbc -pl datacooker-s3direct
