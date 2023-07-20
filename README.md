# Tessellate

A command line tool for reading and writing data to/from multiple locations and across multiple formats.

This project is under active development and many features are considered alpha.

Please do play around with this project in order to provide early feedback, but do expect things to change until we hit
1.0 release.

All final and WIP releases can be found here:

- https://github.com/ClusterlessHQ/tessellate/releases

## About

A primary activity of any data-engineering effort is to format and organize data for different access patterns.

For example, logs frequently arrive as lines of text, but are often best consumed as structured data. And different
stakeholders may have different needs of the log data, so it must be organized in different ways that support those
needs.

Tessellate was designed to support data engineers and data scientists in their efforts to manage data.

Tessellate may be used from the command line, but also natively supports the
[Clusterless](https://github.com/ClusterlessHQ/clusterless) workload model.

## Features

### Pipeline definition

Tessellate pipelines are defined in JSON files.

For a copy of a template pipeline JSON file, run:

```shell
tess --print-pipeline > pipeline.json
```

Some command line options are merged at runtime with the pipeline JSON file. Command line options take precedence over
the pipeline JSON file.

Overriding command line options include

- `--inputs`
- `--input-manifest`
- `--input-manifest-lot`
- `--output`
- `--output-manifest`
- `--output-manifest-lot`

In order to embed system properties, environment variables, or other provided intrinsic values, [MVEL
templates](http://mvel.documentnode.com) are supported.

Current context values supported are:

- Environment variables
- System properties
- Pipeline source properties
- Pipeline sink properties

For example:

- `@{env['USER']}` - resolve an environment variable
- `@{sys['user.name']}` - resolve a system property
- `@{sink.manifestLot}` - resolve a sink property from the pipeline JSON definition

Used in a transform to embed the current `lot` value into the output:

```json
{
  "transform": [
    "@{source.manifestLot}=>lot|string"
  ]
}
```

### Supported data formats

- `text/regex` - lines of text parsed by regex
- `csv` - with or without headers
- `tsv` - with or without headers
- [Apache Parquet](https://parquet.apache.org)

Regex support is based on regex groups. Groups are matched by ordinal with the declared fields in the schema.

Provided named formats include:

- AWS S3 Access Logs
  - named: `aws-s3-access-log`
  - https://docs.aws.amazon.com/AmazonS3/latest/dev/LogFormat.html

Usage:

```json
{
  "source": {
    "schema": {
      "name": "aws-s3-access-log"
    }
  }
}
```

### Supported data locations/protocols

- `file://`
- `s3://`
- `hdfs://`

### Supported path and filename patterns

- Path partitioning - data can be partitioned by intrinsic values in the data set.
  - partitioning can be named, e.g. `year=2023/month=01/day=01`, or
  - unnamed, e.g. `2023/01/01`
- Filename metadata - `[prefix]-[field-hash]-[guid].parquet`
  - `prefix` is `part` by default
  - `field-hash` is a hash of the schema: field names, and field types
  - `guid` is a random UUID or a provided value

### Supported single field transforms

- insert - insert a literal value into a field
  - `value=>intoField|type`
- coerce - transform a field to a new type
  - `field|newType`
- copy - copy a field value to a new field
  - `fromField+>toField|type`
- rename - rename a field, optionally coercing its type
  - `fromField->toField|type`
- discard - remove a field
  - `field->`

### Supported types

- `String`
- `int` - `null` coerced to `0`
- `Integer`
- `long` - `null` coerced to `0`
- `Long`
- `float` - `null` coerced to `0`
- `Float`
- `double` - `null` coerced to `0`
- `Double`
- `boolean` - `null` coerced to `0`
- `Boolean`
- `DateTime|format` - canonical type is `Long`, format defaults to `yyyy-MM-dd HH:mm:ss.SSSSSS z`
- `Instant|format` - canonical type is `java.time.Instant`, supports nanos precision, format defaults to ISO-8601
  instant format, e.g. `2011-12-03T10:15:30Z`
- `json` - canonical type is `com.fasterxml.jackson.databind.JsonNode`, supports nested objects and arrays

## To Build

So that the Cascading WIP releases can be retrieved, to `gradle.properties` add:

```properties
githubUsername=[your github username]
githubPassword=[your github password]
```

> ./gradlew installDist

## To Run

```shell
./tessellate-main/build/install/tess/bin/tess --help
```

To print a project file template:

```shell
tess --print-pipeline
```

Documentation coming soon, but see the tests for usage. 
