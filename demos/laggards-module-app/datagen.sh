#!/bin/bash

set -x
config=$1
export CLASSPATH=`hadoop classpath`:`pwd`/lib/*:`pwd`/app/laggards-module-app-3.3.0-SNAPSHOT.jar
if [[ "$config" == "" ]]
then
  java com.datatorrent.demos.laggards.datagen.PopulateKafka
else
  configpath=`pwd`/kafka-${config}.prop
  java com.datatorrent.demos.laggards.datagen.PopulateKafka $configpath
fi

