#!/usr/bin/bash

# source-debezium
cd source-debezium || exit
./gradlew buildImages

# sink-jdbc
cd ../sink-jdbc || exit
./gradlew buildImages

# sink-mongodb
cd ../sink-mongodb || exit
./gradlew buildImages

# sink-null
cd ../sink-null || exit
./gradlew buildImages
