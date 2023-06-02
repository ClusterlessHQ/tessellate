# Tessellate

A command line tool for reading and writing data to/from multiple locations and across
multiple formats.

## About

A primary activity of any data-engineering effort is to format and organize data for different access patterns.

For example, logs frequently arrive as lines of text, but are often best consumed as structured data. And different
stakeholders may have different needs of the log data, so it must be organized in different ways that support those
needs.

Tessellate was designed to support data engineers and data scientists in their efforts to manage data.

Tessellate may be used from the command line, but also natively supports the
[Clusterless](https://github.com/ClusterlessHQ/clusterless) workload model.

## Features

## To Build

> ./gradlew installDist

## To Run

> ./main/build/install/tess/bin/tess --help
