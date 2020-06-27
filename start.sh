#!/bin/bash

if [ $SERVER_PORT ];
then
   ./tail-based-sampling -port $SERVER_PORT
else
   ./tail-based-sampling -port 8000
fi
tail -f start.sh

