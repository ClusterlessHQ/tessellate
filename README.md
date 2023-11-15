# Tessellate

A command line tool for reading and writing data to/from multiple locations and across multiple formats.

This project is under active development and many features are considered alpha.

Please do play around with this project in order to provide early feedback, but do expect things to change until we hit
1.0 release.

Documentation can be found at: https://docs.clusterless.io/tessellate/1.0-wip/index.html

All tessellate releases are available via [Homebrew](https://brew.sh):

```shell
brew tap clusterlesshq/tap
brew install tessellate
tess --version
```

Alternatively, you can download the latest releases from GitHub:

- https://github.com/ClusterlessHQ/tessellate/releases

## About

A primary activity of any data-engineering effort is to format and organize data for different access patterns.

For example, logs frequently arrive as lines of text, but are often best consumed as structured data. And different
stakeholders may have different needs of the log data, so it must be organized in different ways that support those
needs.

Tessellate was designed to support data engineers and data scientists in their efforts to manage data.

Tessellate may be used from the command line, but also natively supports the
[Clusterless](https://github.com/ClusterlessHQ/clusterless) workload model.

## Building

So that the Cascading WIP releases can be retrieved, to `gradle.properties` add:

```properties
githubUsername=[your github username]
githubPassword=[your github personal access token]
```

See creating a personal access
token [here](https://docs.github.com/en/github/authenticating-to-github/creating-a-personal-access-token).

```shell
./gradlew installDist
```

```shell
./tessellate-main/build/install/tessellate/bin/tess --help
```

Documentation coming soon, but see the tests for usage. 
