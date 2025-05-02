docker rm hive4

docker run -d \
    -p 10000:10000 \
    -p 10002:10002 \
    -v ./:/lab4 \
    --env SERVICE_NAME=hiveserver2 \
    --name hive4 apache/hive:4.0.1