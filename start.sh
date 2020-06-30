#!/bin/bash

if [ $SERVER_PORT ];
then
   go run ./src/multiEntry.go -port $SERVER_PORT
else
   go run ./src/multiEntry.go -port 8000
fi
#tail -f start.sh
#java -Dserver.port=9000 -DcheckSumPath=F:/nginx-1.13.9/html/checkSum.data -jar ./scoring-1.0-SNAPSHOT.jar

