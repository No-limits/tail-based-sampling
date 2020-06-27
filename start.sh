#!/bin/bash

if [ $SERVER_PORT ];
then
   go run ./src/multiEntry.go -port $SERVER_PORT
else
   go run ./src/multiEntry.go -port 8000
fi
#tail -f start.sh

