#!/bin/bash
jobid=$(curl --location 'http://192.168.137.11:8081/jobs')
stat=""
x=0
until [ "$stat" = "RUNNING" ]
do
   x=$((x+1))
   stat=$(echo "$jobid" | cut -d $'"' -f$x)
   if [ "$stat" = "" ]
   then
       sleep 1s
       jobid=$(curl --location 'http://localhost:8081/jobs')
       x=0
   fi
done
x=$((x-4))
jobid=$(echo "$jobid" | cut -d $'"' -f$x)
sleep 20s
docker restart "$1"
sleep 60s
docker exec "$1" /data/shared/flink/bin/taskmanager.sh start
curl --location --request PATCH "http://192.168.137.11:8081/jobs/$jobid?mode=cancel"
