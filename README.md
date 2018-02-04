# S3 Log Sync

A simple application for syncing AWS S3 access logs from S3 to disk or Elasticsearch.

## About

Specifically this project is intended to optimize the reliable retrieval of Amazon S3 access logs, parse them, and 
organize them somewhere useful.

More broadly, it is intended to showcase using Cascading local mode as a toolkit for building ETL applications.

This project was built with:

* [Cascading](http://www.cascading.org)
* [Cascading-Local](http://www.heretical.io/projects/cascading-local/) 

## To Build

    > gradle shadowJar

## To Run

To sync the logs from an S3 bucket to disk, partitioning the data by request date and retrieved key 
(`2012/Jul/13/retrieved-file.tgz/`): 

    > java -jar s3-log-sync-shaded-1.0.0-wip-dev.jar --input s3://logs.example.org/files.example.org \
      --input-checkpoint output/checkpoints --output output/logs --output-format json --partition-on-key

To sync logs into an Elasticsearch index while parsing the request URI query string into a JSON tree:

    > java -jar s3-log-sync-shaded-1.0.0-wip-dev.jar --input s3://logs.example.org/files.example.org \ 
      --input-checkpoint output/checkpoints --output es://localhost:9200/logs/s3 --output-format json \
      --parse-query-string