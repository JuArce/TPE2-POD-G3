#!/bin/bash

tar -xf "server/target/tpe2-g3-server-1.0-SNAPSHOT-bin.tar.gz" -C "server/target/"
java -cp 'server/target/tpe2-g3-server-1.0-SNAPSHOT/lib/jars/*'  "ar.edu.itba.pod.server.Server" "$@"
