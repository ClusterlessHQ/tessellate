# Tessellate guidelines

Living engineering knowledge for the tessellate codebase (`tess`). Skim before
any non-trivial change. Two sections:

- **Project conventions** — how the codebase is shaped. Architectural
  invariants that survive across resolutions, and the DX contract new code
  is held to.
- **Resolution patterns** — workflow, triage filters, and implementation
  gotchas. The "I would have spent two hours on this without the prior
  context" file.

When a resolution teaches something a future session needs that this file
doesn't already say — a new convention, a non-obvious gotcha, a vocabulary
distinction, a debug technique, an "adding X touches N sites" recipe — add a
terse declarative bullet to the relevant section. Don't record specific bug
details (those live in the commit). One-time decisions don't belong here
either.

Code is the source of truth. The Antora docs and `docs/OPERATIONS.md` are
partly stale (see *docs/ drift* below); when they disagree with the code, the
code wins and the doc gets fixed. References use `path::Symbol` rather than
line numbers so they survive edits; class references are relative to
`tessellate-main/src/main/java/io/clusterless/tessellate/` unless a path is
given.

---

## Project conventions

### Process and repo

- **Commit style: Conventional Commits**, enforced by `.githooks/commit-msg`
  (`git config core.hooksPath .githooks` once per clone). Types: `feat fix
  docs refactor test chore build ci perf revert`; scope optional (`cli`,
  `pipeline`, `factory`, `parser`, `build`, `deps`, `ci`); description
  lowercase, imperative, no trailing period. No `Co-Authored-By` / `Claude-*`
  trailers or Claude Code links — authorship is the Author header. Every
  pushed commit must be signed (`.githooks/pre-push`);
  `.githooks/reference-transaction` refuses unsigned commits locally when
  `commit.gpgsign=true`.
- **Only the version branch publishes.** `.github/workflows/wip.yml` runs
  `check` on every `wip-*` push. Only `wip-<release.major>` (today `wip-1.0`,
  read from `version.properties`) runs the `release` job: jreleaser GitHub
  release, Homebrew tap, Docker Hub (`clusterless/tessellate`, versioned tag
  + `latest-wip`), and the Netlify docs build hook. Sub-wip branches
  (`wip-1.0-<topic>`) are test-only.
- **Every release is a new GitHub release.** `buildRelease = false` makes the
  version `1.0-wip-<GITHUB_RUN_NUMBER>`, so each publish creates tag
  `v1.0-wip-<n>`. `overwrite` only matters for a re-run of the same number.
- **Docs-only pushes run nothing.** `paths-ignore` skips `**.md`, `**.adoc`,
  and `**.txt`, so an Antora-only change is not deployed until the next code
  push to the version branch.
- **Bumping `release.major` touches 5 sites:** `version.properties`,
  jreleaser `release.github.branch` in `tessellate-main/build.gradle.kts`,
  `util/Versions::WIP`, `tessellate-main/src/main/antora/antora.yml`
  `version`, and the README docs URL.
- **NEVER `git stash` in a worktree.** The stash stack is shared across
  worktrees, so a pop can restore another session's stash. To revert a file
  for a red/green check: `cp <path> /tmp/save && git show <rev>:<path> >
  <path>` … run … `cp /tmp/save <path>`, then `cmp`.
- **Scratch lives in `_*` directories** (gitignored by `_*`). Don't
  `git add -f` them unless asked. Don't point a manifest-producing sink at a
  `_*` path (see *Manifests*).

### Architecture: how a `tess` invocation runs

- **One process, one flow.** `Main::main` handles `--help`, `--version`, and
  `--show-source`/`--show-sink` itself, then calls `CommandLine.execute`, which
  re-parses argv. `Main::call` runs `PipelineOptionsMerge::merge`, then
  `Pipeline::build`, then `Pipeline::run`.
- **Always Cascading local mode.** `Pipeline::build` uses
  `LocalFlowConnector`. Hadoop is only the filesystem/IO layer: `Hfs` taps
  wrapped in `LocalHfsAdaptor` (`FSFactory::createTap`). There is no
  MapReduce and no cluster, so everything runs in one JVM and joins are
  memory-bound.
- **Merge order** (`PipelineOptionsMerge::merge`):
  1. Read the pipeline JSON (comments allowed).
  2. Resolve relative `file:` URIs against the pipeline file's directory. Only
     `/source/inputs/*`, `/source/manifest`, and `/sink/output` are resolved;
     `errorPath` and named `sources.*` inputs are not. CLI-supplied paths stay
     relative to cwd.
  3. Apply CLI options through `buildSpec`/`argumentLookups`; the CLI wins
     over the file.
  4. Overlay the named schema `schemas/<name>.json` from the classpath
     (`loadAndMerge`). This uses Jackson update semantics: **the resource's
     scalars overwrite inline values and arrays are appended**, so an inline
     `format` or `declared` cannot override a named schema.
  5. Evaluate the whole JSON text as an MVEL template.
  6. Bind to `model/PipelineDef`.
- **Parsing happens at bind time.** `model/Field` parses its declaration in
  its constructor. `model/Transform` parses each statement through
  `StatementParser::parse` in `addStatement`. A syntax error is therefore a
  Jackson bind failure, not a pipeline failure.
- **The whole pipeline file is an MVEL template.** Any `@{…}` / `@code{…}`
  anywhere, including regex patterns and literals, gets evaluated. The context
  comes from `LiteralResolver::context(source, source)`, so **`@{sink.*}`
  resolves against the source map**.
- **Factory dispatch is by URI scheme, then format parent** (`TapFactories::findFactory`):
  - **Scheme to Protocol.** The scheme goes through `Protocol.valueOf`, so it
    must equal an enum name: `s3a://` throws. `s3://` is served by S3A via
    `fs.s3.impl` (`FSFactory::applyGlobalProperties`).
  - **Several factories for one protocol.** They are disambiguated by
    `format.parent()` and compression. `json` and `regex` have parent
    `text`, so json on s3/hdfs is built by `TextFSFactory` and
    `JSONFSFactory` is never selected; it only feeds `--show-*` listings.
  - **No output means stdout.** With no output URI the sink is
    `StdOutFactory`.
- **Format inference is source-only.** `TapFactories::findSourceFactory`
  infers format from the first input's extension when unset. A sink format is
  never inferred.
- **Scheme construction is copy-pasted per factory.** The format switch exists
  in `LocalDirectoryFactory::createScheme`, `StdOutFactory::createScheme`, and
  `TextFSFactory::createScheme`. Each has `default:` falling through to
  `text`, so a missing case silently becomes `TextLine`.
- **Text/regex header skipping is a hack in `Pipeline::build`.** It filters
  `num == 0` and depends on the factories emitting `num, line` when
  `embedsSchema` is true. The two sides must stay mirrored.
- **Transforms** dispatch in `pipeline/Transformer::resolve` on the operator
  (`""` coerce, `=>` insert, `+>` copy/eval, `->` discard/rename/eval).
  Filters support regex only (`~/…/`). Evaluation is intrinsics only
  (`^name{…}`, registered in `pipeline/Intrinsics`).
- **Joins:** the primary source is the single *rhs* relation name
  (`Pipeline::findPrimarySource`); *lhs* relations come from `sources`.
  `Transformer::handleJoin` builds `HashJoin(lhs, rhs)`. Check which side
  Cascading accumulates before relying on `joins.adoc`'s "lhs fits in memory"
  claim.
- **Error traps:** `errorPath` becomes a gzip CSV sink with prefix
  `errors-source` / `errors-sink` and throwable message, stack, and element
  trace (`Pipeline::createTrap`). `Pipeline::createJoinSources` registers
  join-source traps under the `HEAD` key, replacing the primary source's trap.
- **Manifests are fed by filesystem interception.** `factory/Observed` is a
  static singleton. It records only writes that go through the
  `ObserveS3AFileSystem` / `ObserveLocalFileSystem` `create` overloads, and it
  skips any path containing `/_`.
  - `ManifestWriter` is only attached by `FSFactory`. **`file://`
    non-parquet sinks (`LocalDirectoryFactory`) never write a manifest.**
  - An empty input manifest short-circuits to writing an `empty` sink
    manifest (`Pipeline.State.EMPTY_MANIFEST`).
- **AWS configuration** is built in `FSFactory::applyAWSProperties`:
  - **Credentials:** `factory/hdfs/aws/DefaultChainCredentialsProvider` (the
    SDK v2 default chain) first, then S3A's
    `CredentialProviderListFactory.STANDARD_AWS_PROVIDERS`. S3A instantiates
    and closes providers per filesystem, so a provider must not wrap a shared
    instance like `DefaultCredentialsProvider.create()` (closing one
    filesystem would close it for all).
  - **Assumed role:** the `fs.s3a.assumed.role.arn` system property (via
    `TESS_OPTS`) **beats** `--input-/--output-aws-assumed-role-arn`, which
    beats `--aws-assumed-role-arn`; the system property is applied last.
  - **Endpoint:** env `AWS_S3_ENDPOINT` **beats** the `fs.s3a.endpoint`
    system property, which beats `--aws-endpoint` (the CLI value is only the
    fallback default).
  - **Static state:** `FSFactory`'s static block sets a UGI login user so
    Hadoop works in containers.

### CLI / DX contract

Adopted from the retrofit/clusterless CLI contract. Existing code does not yet
meet all of it; **new and touched code must**.

- **Never silent.** Logging is OFF at verbosity 0 (the `util/Verbosity`
  static block); `-v` is INFO, `-vv` DEBUG. A failure reported only through
  `LOG.error` is invisible by default. Log for diagnosis; print to stderr for
  the user.
- **stdout is data, stderr is everything else.** Today `logback.xml`'s
  console appender, `--metrics-print` (`MetricsPrinter.printStream`), and the
  usage-on-parse-error all write to stdout. That corrupts data when the sink
  is stdout (no `-o`). Don't build on it; fix it when touched. `-d/--debug`
  uses Cascading `Debug`.
- **Machine outputs are contracts.** These are stable JSON on stdout: adding
  is safe, renaming or removing is breaking.
  - `--show-source` / `--show-sink` `{formats,protocols,compression}`
  - `--print-pipeline [simple|all]` (`simple` = `@JsonSimpleView` fields)
  - `--print-output-schema` with `--print-format JSON|SQL` (Athena dialect,
    `printer/TypeMap`)
- **Exit codes today are ad hoc:**

  | Case | Output | Exit |
  |---|---|---|
  | `MissingParameter` / `UnmatchedArgument` | message on stderr, usage on stdout | 255 (`System.exit(-1)`) |
  | Any other picocli parse error, e.g. a bad enum value | uncaught out of `parseArgs` | 1 |
  | Exception inside `call` | picocli's default handler prints the stack trace | 1 |
  | Missing pipeline file, Cascading flow failure (`Pipeline::handleCascadingException`) | message on stderr | 255 |

  The try/catch around `execute` in `Main::main` never fires, so its
  `-v`-gated stack trace is dead code. New code returns deliberate
  non-negative codes and does not add `System.exit` calls.
- **Option naming.** Long options are kebab-case. AWS options come in
  triads: `--input-aws-*`, `--output-aws-*`, and global `--aws-*`; the
  per-side value wins. Short flags are taken: `-i -o -m -t -l -p -d -v`. An
  option that sets a pipeline value must go through `PipelineOptionsMerge`,
  never be read directly by a factory.
- **Environment variables:**
  - New tessellate env vars take the `TESS_` prefix. `TESS_OPTS` is already
    owned by the Gradle start script (with `JAVA_OPTS`); use it for `-D`
    (`fs.s3a.session.token`, `fs.s3a.assumed.role.arn`, proxy host/port).
  - `TESS_INSTANT_TYPE_FORMAT` and `TESS_DATE_TYPE_FORMAT` are read in
    `FieldsParser::resolveType` whenever an `Instant`/`DateTime` is declared
    without a param. The value uses field-type-param syntax
    (`FieldParser::parseFieldTypeParam`). They apply everywhere: source,
    sink, partitions, named schemas, transforms, `--output-fields`. So the
    same pipeline file can mean different things in different environments.
  - AWS: `AWS_S3_ENDPOINT`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`.
- **Messages:** lowercase, `what: value` form ("pipeline file does not
  exist: <path>"), naming the offending file/URI/field. Log messages match
  that style.
- **Help lives only in picocli annotations.** There is no generated CLI
  reference; the Antora pages hand-copy option names and drift (see below).

### Pipeline JSON and persisted formats — changes here break users

Pipeline files are user-authored and have no schema version. Output layout
and manifests are read by downstream tools. Treat everything below as a
contract.

- **Readers are strict.** Unknown properties are rejected (Jackson default,
  no `ignoreUnknown`). Duplicate keys are rejected
  (`FAIL_ON_READING_DUP_TREE_KEY`). `//` and `#` comments are accepted
  (`JSONUtil.CONFIG_READER`). Adding a model field makes new pipeline files
  unreadable by older `tess`.
- **Model binding is field-based** (`model/Model` `@JsonAutoDetect(fieldVisibility = ANY)`).
  Defaults live in field initializers. Builders are hand-written: a new field
  needs the field, accessor, `Builder.withX`, and the copy in `build()`.
- **Enum constant names are the JSON/CLI values:** `util/Format`,
  `util/Compression` (including the misspelled `lzstdzo`), `util/Protocol`,
  `parser/ast/JoinType`, `Main.Show`, `Main.PrintScope`,
  `PrintOptions.PrintFormat`. Don't rename them.
- **Field and statement syntax is user syntax** (`parser/FieldParser`,
  `parser/StatementParser`): `name|Type|param`, `+>`, `->`, `=>`, `~/re/`
  (`//` escapes `/`), `^name{k:v}`, `lhs(…) rhs(…) +inner{}`.
  `model/Transform` serializes via each statement's `toString`
  (`@JsonValue`), so every AST node must render re-parseable text or
  `--print-pipeline` output stops round-tripping.
- **Named schemas** (`src/main/resources/schemas/*.json`, today only
  `aws-s3-access-log`) are referenced by name from user pipelines. Changing
  their `declared` list changes users' output columns.
- **Output layout:**
  - **Partition paths:** `name=value/` when `namedPartitions` (default
    true), else `value/`.
  - **Hadoop-path part files:** `<prefix>[-<fieldsHashHex>][-<guid>]-NNNNN-NNNNN`
    (`FilesFactory::getPartFileName`, `FSFactory::applySinkProperties`).
  - **Local part files:** use the same prefix through `PrefixedDirTap`.
  - **Defaults:** prefix `part`. The fields hash is
    `Integer.toHexString(Fields.hashCode())`, so a Cascading upgrade that
    changes `Fields.hashCode` renames files and looks like a schema change
    downstream.
- **Manifests** mirror the clusterless manifest store; change both together.
  - JSON `{state, comment, lotId, uriType: "identifier", uris}`
    (`ManifestWriter`).
  - The template expands `{lot}` and `{state}` and discards `{/attempt*}`.
  - States are `complete` / `empty`. `ManifestReader::isEmptyManifest` keys
    on the literal `state=empty` in the manifest URI, so the `state={state}`
    template convention is load-bearing.
  - A manifest template without a lot throws.
- **Instant interval units** (`Twelfths`, `Fourths`, …) come from
  `clusterless-commons` `IntervalUnits`, the vocabulary shared with
  clusterless lots. Bump commons in lockstep; its source is the sibling
  `commons` repo.

### Build and test

- **Toolchain:** Gradle wrapper 8.14.5 (checksum-pinned), Java 11 toolchain
  via the foojay resolver (`settings.gradle.kts`). **Gradle itself needs
  Java 17+**: foojay 1.0.0 is Java 17 bytecode and loads into the Gradle JVM.
  CI installs Temurin 11 and 17, runs Gradle on 17, and points the toolchain
  at 11 with `-Porg.gradle.java.installations.fromEnv=JAVA_HOME_11_X64`. The
  first build needs network and GitHub Packages credentials.
- **Dependency versions are inline** in `tessellate-main/build.gradle.kts`
  `dependencies {}` as local `val`s (`cascading`, `parquet`,
  `hadoop3Version`, `jackson`, `jupiter`, …; `awsSdk2` sits above the block
  because it is shared by main and the integration tests). There is no
  catalog or constraints block. A global `configurations.implementation`
  exclude list trims Hadoop's transitive tree (yarn, jetty, jersey, protobuf,
  log4j/reload4j), so a Hadoop bump may need new excludes or may be missing a
  class at runtime.
- **AWS SDK v2 is declared module by module.** `hadoop-aws` depends on the
  `software.amazon.awssdk:bundle` jar, which is excluded as too large; the
  modules S3A uses are declared from the SDK BOM (`s3`, `apache-client`,
  `netty-nio-client` and `s3-transfer-manager` for copy/rename and upload,
  `sts` for assumed roles). Netty is excluded from the Hadoop artifacts only,
  so the SDK's Netty client stays. S3A's `ConfigureShadedAWSSocketFactory`
  needs the bundle's shaded httpclient; without it
  `fs.s3a.ssl.channel.mode` is ignored (logged at debug). Client-side
  encryption would also need `kms`.
- **Cascading comes from GitHub Packages.** The repository is
  `https://maven.pkg.github.com/cwensel/*`, restricted to
  `net.wensel:cascading-*:*-wip-*`.
  - Credentials: `githubUsername` / `githubPassword` Gradle properties, or
    env `USERNAME` / `GITHUB_TOKEN`. CI writes `gradle.properties` from
    `vars.GRADLE_PROPERTIES`.
  - A `401 Unauthorized` on `cascading-*.pom` means the stored token is stale.
    `-PgithubPassword="$(gh auth token)"` overrides it for one run (the `gh`
    token carries `read:packages`).
  - `mavenLocal()` is consulted only for `*-wip-dev` Cascading builds.
- **Version stamping:** the `writeVersionProperties` task generates
  `version.properties` (`release.full`) into its own resource srcDir with the
  version as a declared input; `util/Versions::clsVersion` falls back to
  `Versions.WIP` when the resource is absent. `VersionPropertiesTest` guards
  against the resource being dropped.
- **Verify ladder, cheapest first:**
  - `./gradlew compileJava compileTestJava`
  - `./gradlew test --tests '<Pattern>'` — one test.
  - `./gradlew test` — all unit tests; no Docker.
  - `./gradlew integrationTest` — LocalStack S3 via Testcontainers; **needs
    Docker**.
  - `./gradlew check` / `clean check` — `check` explicitly depends on
    `integrationTest`, so it needs Docker too. This is the lane to run after
    major tasks.
  - `./gradlew installDist` — runnable CLI at
    `tessellate-main/build/install/tessellate/bin/tess`.
  - `./gradlew release` — jreleaser with `dryrun = false`: signs and
    **publishes** the GitHub release, Homebrew, and Docker images. **Never
    run locally.**
- **Test layout:**
  - **Unit tests** (`src/test`) drive `PipelineOptionsMerge` + `Pipeline`
    in-process (`PipelineTest`, `JoinPipelineTest`). Inputs come from
    `@PathForResource`; outputs go to `@PathForOutput`, which is
    `build/output/<class>/<method>/…` and wiped per test (`testFixtures`
    `junit/ResourceExtension`). Env-dependent tests use system-stubs
    `EnvironmentVariables` (`FieldParserTest`).
  - **Integration tests** (`src/integrationTest`): one class,
    `PipelineIntegrationTest`, with no base class. It uses a static
    `LocalStackContainer`, points S3A at it via system-stubs env
    (`AWS_S3_ENDPOINT`, keys, region), and gets output URIs from
    `@URLForOutput(scheme = "s3", host = TEST_BUCKET)`.
  - Testcontainers fails loudly without Docker. Keep it that way; a test
    that skips when its environment is missing looks green while proving
    nothing. Docker 29 needs Testcontainers 2.x; 1.x reports "Could not find
    a valid Docker environment".
- **Coverage is thin where DX lives.** No test runs `Main`: parse errors,
  exit codes, and stdout/stderr separation are untested. Any CLI change adds
  a test through the real entry path, asserting stdout, stderr, and exit code
  (system-stubs can catch `System.exit`).
- **Style:** no formatter or linter; match surrounding code. MPL-2.0 header
  on every file (copy an existing one). Nullability via
  `org.jetbrains:annotations`. No `module-info.java`.

### Modernization hazards (this branch's purpose)

- **Java 11 appears in three places:** the `java.toolchain`, both CI jobs'
  `setup-java` list and `JAVA_HOME_11_X64` flag, and the jreleaser
  Docker/Homebrew packaging. Move them together and inspect the generated
  `build/jreleaser` output after a bump. The Gradle JVM (17 in CI) is
  separate and must stay at or above what the settings plugins require.
- **Gradle 9:** `./gradlew help --warning-mode all` reports no deprecations
  on Gradle 8.14.5. Check jreleaser plugin compatibility before moving the
  wrapper.
- **Hadoop sets the JDK ceiling.** Hadoop 3.3.x calls
  `Subject.getSubject`, which throws on JDK 24+; 3.4.3 and 3.5.0 carry the
  fix (HADOOP-19212). On 3.4.3 the suites pass on JDK 11, 21, 25, and 27.
  Hadoop 3.5 needs a Java 17 floor. On any Hadoop bump, re-verify that
  `ObserveS3AFileSystem`'s `create` overrides are still the write path, or
  manifests silently go empty.
- **The Cascading `-wip-` pin** is only resolvable from GitHub Packages. The
  APIs used are wip-level (`LocalHfsAdaptor`, `TypedParquetScheme`,
  `JSONTextLine`, `PartitionTap`, anonymous `Hfs` subclasses overriding
  `create` / `getChildIdentifiers` / `sourceConfInit`). Parquet is pinned
  separately (`parquet` val) and must stay compatible with
  `cascading-hadoop3-parquet`.
- **LocalStack stays pinned at `localstack/localstack:2.1.0` — do not bump
  it.** Since 2026-03-23 every current LocalStack image requires an auth
  token; pinned older tags still run without one. AWS SDK v2 2.30+ sends
  flexible checksums by default, which old LocalStack rejects on puts. S3A
  3.4.3 builds its clients with `WHEN_REQUIRED` unless
  `fs.s3a.checksum.generation` is set, so S3A writes work; a test that puts
  through its own SDK client must set `AWS_REQUEST_CHECKSUM_CALCULATION` /
  `AWS_RESPONSE_CHECKSUM_VALIDATION=WHEN_REQUIRED`, as clusterless does.
- **lz4-java** moved from `org.lz4` to `at.yawk.lz4`; declare the new
  coordinates (the old ones are relocation POMs) and keep only one on the
  classpath.
- **Aging parsers:** `jparsec` 3.1 (grammar in `StatementParser` /
  `FieldParser`) and `mvel2` (pipeline templating) are effectively
  unmaintained; a replacement changes user syntax edge cases.
- **`javax.xml.bind:jaxb-api`** is there for Hadoop on Java 9+; keep it
  until Hadoop no longer needs it.

### Declared-but-unwired scaffolding

These exist in the model or CLI but have no working path. Don't build on
them, and don't "fix" them piecemeal:

- `--aws-region`, `--input-aws-region`, `--output-aws-region`:
  `AWSOptions::aswRegion` (sic) is never called, so the region comes from
  SDK/S3A defaults.
- `PipelineDef.aws` (`model/AWS` endpoint, region, role) is never read;
  AWS is configured only by CLI, env, and system properties.
- `Source.lines` (`LineOptions` sample/max) and `Source.select` are never
  read. `Schema.documentation` is informational only.
- `Protocol.http` / `https`: no factory registers them. `Protocol.stdout` /
  `-` is never produced by dispatch.
- `JSONFSFactory` is unreachable by dispatch (see *Architecture*).
- `IntrinsicBuilder::params` is informational: intrinsic params are never
  validated, so unknown keys are silently ignored.

### docs/ authoring

- **User docs** live in `tessellate-main/src/main/antora` (component
  `tessellate`, version `1.0-wip`) and are published to docs.clusterless.io
  by the Netlify hook in the release job. Root `docs/OPERATIONS.md` is a
  work-in-progress design note, not user docs.
- **Standalone.** No links into gitignored `_*` folders or to sibling-repo
  support material.
- **docs/ drift.** Verify against code before citing:
  - **`pipeline.adoc`:** its override list names `--inputs` and
    `--output-manifest` (the real options are `-i/--input` and
    `-t/--output-manifest-template`) and omits `--output-fields`,
    `--output-format`, and `--*-errors`. Its `@{sink.manifestLot}` example
    actually resolves the source.
  - **`quickstart.adoc`:** claims `http(s)://` reads; there is no factory
    for them.
  - **`source-sink.adoc`:** its format list omits `json` and `delimited`.
  - **`joins.adoc`:** calls `inner` the default, but the grammar requires
    an explicit `+type{}`.
  - **`OPERATIONS.md`:** describes `!{java}` expressions and `@[pointer]`
    operators that are not implemented.

---

## Resolution patterns

### Workflow

- **Build the CLI once and reproduce verbatim.** `./gradlew installDist`,
  then run the reproducer from a temp dir with `-vv` (logs are off by
  default and go to stdout, so separate them from data with `-o`).
- **Look at the merged pipeline first.** `tess -p <file> [overrides]
  --print-pipeline all` prints the post-merge, post-MVEL, named-schema-overlaid
  definition without running it. Most "tess ignored my value" reports are
  visible here.
- **Reproduce in a unit test, not through `Main`.** Build a `PipelineDef`
  (or read JSON), run `PipelineOptionsMerge::merge(JsonNode)`, then
  `new Pipeline(options, def).run()`, as `PipelineTest` does.
- **Probe, then delete.** For "what does Jackson/Cascading/S3A actually do",
  write a throwaway single-file `Probe.java` outside the repo. Run it with
  `java -cp <jars from ~/.gradle/caches/modules-2/files-2.1/…> Probe.java`,
  ground-truth the assumption, and delete it.
- **Look up idioms before inventing a shape.** Code → **semble first**
  (`semble search "<behavior>" .`). Cascading 4.6 wip source is not checked
  out anywhere current; use `javap` against the `net.wensel` jars in the
  Gradle cache. Sparse checkouts in `../thirdparty/`: `hadoop` at
  `rel/release-3.4.3` (hadoop-aws main and site docs, hadoop-common `fs` and
  `security`, hadoop-project) and `aws-sdk-java-v2` at `2.55.6`. Re-pin with
  `git -C ../thirdparty/<repo> checkout <tag>` when the dependency is bumped.
- **Run `integrationTest` for anything touching `FSFactory`, S3A, Hadoop,
  Parquet, AWS, or Cascading versions.** Unit tests only exercise `file:`
  paths.
- **Diff the resolved classpath for build-script refactors.**
  `./gradlew :tessellate-main:dependencies --configuration <cfg>` before and
  after; a refactor that should be behavior-neutral must produce the same
  list.

### Triage filters

- **"tess ignored my option/value"** — check the merge order in
  *Architecture*:
  - A named schema's scalars beat inline values.
  - `AWS_S3_ENDPOINT` and `-Dfs.s3a.endpoint` beat `--aws-endpoint`;
    `-Dfs.s3a.assumed.role.arn` beats the role options.
  - Region flags are unwired.
  - Relative CLI paths resolve against cwd, file paths against the pipeline
    file.
- **"Output on stdout is garbled"** — log lines (`-v`) or `--metrics-print`
  are interleaved with stdout-sink data.
- **"Manifest is empty or missing":**
  - The sink went through `LocalDirectoryFactory` (`file://` non-parquet).
  - The output path contains `/_`.
  - The write bypassed the overridden `create` overloads.
  - The template lacks `state={state}`.
- **"no factory found" / `No enum constant …Protocol`** — the URI scheme
  (`s3a`, `gs`, `http`) isn't a registered `Protocol`, or the
  format/compression pair isn't in any factory's `getFormats()` /
  `getCompressions()`.
- **NPE in `TapFactories::findFactory`** — the sink format is unset on a
  protocol with more than one factory (`file` has two). Sink formats are
  never inferred.
- **"Dates/times parse differently on another machine"** — a
  `TESS_*_TYPE_FORMAT` env var is set there. Also note the defaults differ:
  `DateTime` is `yyyy-MM-dd HH:mm:ss.SSSSSS z`, `Instant` is ISO micros.
- **"`@{…}` or text in my regex/literal vanished"** — MVEL evaluated it.
- **"json behaves differently on s3 than locally"** — different factories
  and mappers: `TextFSFactory` uses `JSONTextLine`'s default mapper,
  `LocalDirectoryFactory` uses `JSONUtil.DATA_MAPPER`.
- **Fixing a bug can unmask a compensating one.** Record the unmasked
  defect separately and note the interaction.

### Implementation patterns

- **Adding a format** touches up to 6 sites:
  - the `util/Format` constant (parent, extensions, `alwaysEmbedsSchema`;
    the parent drives dispatch);
  - `getFormats()` on each supporting factory;
  - the scheme switch in `LocalDirectoryFactory::createScheme`,
    `StdOutFactory::createScheme`, and `TextFSFactory::createScheme`, or a
    new `FSFactory` subclass added to `TapFactories.tapFactories`;
  - `Pipeline::build` if it needs line pre-processing, as text/regex do;
  - `printer/TypeMap` if it introduces types;
  - `source-sink.adoc`.
  Remember the silent `default:` → text fallthrough.
- **Adding a compression:**
  - the `util/Compression` constant (its extension is used for inference);
  - each factory's `getCompressions()`;
  - the codec mapping in `LocalDirectoryFactory::createScheme`,
    `LinesFSFactory::getProperties`, and `ParquetFactory::compressionCodecName`,
    each of which silently defaults to uncompressed;
  - any native codec dependency (see `lz4-java`).
- **Adding a protocol:**
  - a `util/Protocol` constant whose name equals the URI scheme (plus
    `Protocol::fromString`);
  - the factories' `getSourceProtocols()` / `getSinkProtocols()`;
  - for Hadoop filesystems, `fs.<scheme>.impl` pointing at an `Observe*`
    filesystem in `FSFactory::applyGlobalProperties`, or manifests stay
    empty;
  - credentials/endpoint in `FSFactory::applyAWSProperties` if needed.
- **Adding an intrinsic:**
  - an `IntrinsicBuilder` subclass in `pipeline/intrinsic/`, whose name must
    be letters only (`StatementParser.INTRINSIC_NAME` is `many1(IS_ALPHA)`);
  - its Cascading `Function` in `operation/`;
  - registration in the `pipeline/Intrinsics` static block;
  - validation of its own params;
  - a `transforms.adoc` entry and a test beside `FormatFieldsTest`.
- **Adding a CLI option that sets a pipeline value:**
  - the option on `InputOptions`, `OutputOptions`, or `PipelineOptions`;
  - a `PipelineOptionsMerge.buildSpec` `putInto(key, pointer)` **and** a
    matching `argumentLookups` entry under the same key;
  - the pointer in `PipelineOptionsMerge.uris` if it is a path that should
    resolve against the pipeline file;
  - the model field;
  - a `PipelineOptionsMergerTest` case;
  - the override list in `pipeline.adoc`.
- **Adding a model field:**
  - the field, accessor, `Builder.withX`, and the copy in `build()`;
  - `@JsonSimpleView` if it belongs in `--print-pipeline simple`;
  - awareness that older `tess` now rejects pipelines that use it (unknown
    properties fail).
- **Adding a named schema:** `src/main/resources/schemas/<name>.json` with
  `name`, `format`, `pattern`, and `declared`; a merge test like
  `PipelineOptionsMergerTest::fromSchema`; and the list in
  `source-sink.adoc`. Users cannot override its scalars inline.
- **Adding a dependency:** a `val` version and declaration in
  `tessellate-main/build.gradle.kts`. Check it against the global exclude
  list, and add it to `"integrationTestImplementation"` /
  `testFixturesImplementation` if those source sets need it.
- **Bumping Cascading:** change the single `cascading` val (it also drives
  the `:tests` classifier). Run `check` including `integrationTest`, and
  diff a partitioned output's file names for a changed fields hash.
