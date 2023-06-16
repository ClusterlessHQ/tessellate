# Tessellate

A command line tool for reading and writing data to/from multiple locations and across multiple formats.

This project is under active development and many features are considered alpha.

Please do play around with this project in order to provide early feedback, but do expect things to change until we hit
1.0 release.

## About

A primary activity of any data-engineering effort is to format and organize data for different access patterns.

For example, logs frequently arrive as lines of text, but are often best consumed as structured data. And different
stakeholders may have different needs of the log data, so it must be organized in different ways that support those
needs.

Tessellate was designed to support data engineers and data scientists in their efforts to manage data.

Tessellate may be used from the command line, but also natively supports the
[Clusterless](https://github.com/ClusterlessHQ/clusterless) workload model.

## Features

Supported formats:

- `text/regex` - lines of text parsed by regex
- `csv` - with or without headers
- `tsv` - with or without headers
- [Apache Parquet](https://parquet.apache.org)

Supported locations:

- `file://`
- `s3://`
- `hdfs://`

Supported operations:

- Path partitioning - data can be partitioned by intrinsic values in the data set. e.g. (year=2023/month=01/day=01)

## To Build

So that the Cascading WIP releases can be retrieved, to `gradle.properties` add:

```
githubUsername=[your github username]
githubPassword=[your github password]
```

> ./gradlew installDist

## To Run

> ./tessellate-main/build/install/tess/bin/tess --help

To print a project file template:

> tess --print-project

Documentation coming soon, but see the tests for usage. 
