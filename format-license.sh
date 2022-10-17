#!/usr/bin/env bash

#NB! Never automate that action

mvn com.mycila:license-maven-plugin:format -Dlicense.header=./Commons/license.inc -pl CLI -pl Columnar -pl Commons -pl DateTime -pl Geohashing -pl Math -pl Populations -pl Proximity -pl SimpleFilters -pl Spatial
