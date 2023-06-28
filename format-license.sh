#!/usr/bin/env bash

#NB! Never automate that action

mvn com.mycila:license-maven-plugin:format -Dlicense.header=./commons/license.inc -pl cli -pl commons -pl datetime -pl geohashing -pl math -pl populations -pl proximity -pl simplefilters -pl spatial
