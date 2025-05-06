## About this project

**Data Cooker** is a batch and interactive ETL processor that provides simple yet powerful [dialect of SQL](https://pastorgl.github.io/datacooker-etl/TDL.html) (as well as [JSON](https://pastorgl.github.io/datacooker-etl/JSON.html) configs) to perform transformations of Columnar and/or Structured data, which doesn't require strict schema nor data catalog. Built on Apache Spark core with minimal dependencies.
 
Historically it was used in a GIS project, so it has geospatial extensions to its type system, and has been extensively tested and proven reliable by years and petabytes of transformed data in production.

Your contributions are welcome. Official GitHub repo is at https://github.com/PastorGL/datacooker-etl

### Goals

* Provide first class ETL processing with simple, human-friendly SQL. No Python nor Scala code required.
* Run not only on the Spark cluster, but as a local standalone application too. Be callable from any CI/CD or scheduler.
* Require no schema. Require no data catalog. Every field could be defined if needed ad hoc and in place, all data types are inferred by default, and can be explicitly cast if necessary.
* Provide batch processing for production, interactive debugging for development.
* Provide a rich REPL with remote connection support.
* Provide base external storage adapters out of the box.
* Support extensibility via Java API. Keep framework core as small as possible, and write as less code as possible. Allow white-labeling for customized distributions.
* Provide a companion tool with machine-friendly JSON configurations.

### What Data Cooker is NOT

Data Cooker is NOT a replacement for Spark SQL nor extension to it. It is a standalone interpreter.

Data Cooker is a specialized tool designed for efficient handling of data transformations, so it DOES NOT try to emulate a general-purpose RDBMS nor a standards-aligned SQL engine. It has its own dialect of SQL that deals with entire data sets, partitions, and records, but doesn't support record grouping and things like window functions. Because this is analytical workload, NOT ETL specific.

There is NO UI. Data Cooker requires writing scripts, it is NOT a tool for visual arrangement of code blocks dragged on the canvas with mouse.

There is NO explicit integration with any pipelining software.

### Requirements

* Spark 3.5+
* JVM 17+

### Default Storage Adapters and Formats

Default Storages:
* Any Hadoop `FileSystem`-based implementations found in the classpath. By default, vanilla Spark supports local (`file:`), HDFS (`hdfs:`), and S3 (`s3n:`/`s3a:`) URIs via these implementations. On EMR, `s3:` paths are supported with high-performance managed connector.
* Custom `s3Direct` (`s3d:`) adapters that allow connections to any s3-compatible storages with explicit connection parameters.
* JBDC connector that can use any database driver supplied via classpath.

Data container formats (all file-based can be compressed):
* Raw text
* Delimited text
* JSON fragments
* GeoJSON/GPX fragments
* Database's tables

### Default Plugable Modules and Functional Extensions

* Math functions and operators, incl. bitwise
* String functions and operators, incl. Base64 and hashing
* Boolean operators
* Array and Structured object manipulation functions
* Date and Time functions
* Geospatial proximity-based and fencing-based filters
* Uber H3 indexing and other geospatial functions
* General-purpose and socio-demographic statistical indicators calculation extensions

### SQL Dialect Documentation

[Available right here](https://pastorgl.github.io/datacooker-etl/TDL.html), serviced via this repository.

### Key Features That Make Difference

Perform ETL processing blazingly fast! Data Cooker utilizes Spark RDD API with very thin layer of metadata and minimal overhead. Code is meticulously optimized to count every byte and cycle.

Allows addressing data sets on partition level. Unlike higher-level tools Data Cooker completely bypasses unaddressed partitions, and no empty Spark tasks are generated.

Natively supports arbitrary JSON Objects and geospatial right on SQL query level.

SQL dialect has imperative procedural programming extensions: variables, loops, branching operators, functions and procedures. Variables can be defined right in the script, as well as come from command line and environment.

Has extremely powerful REPL with very good debugging abilities. SQL operator `ANALYZE` is particularly fire in that mode.

Custom expression operators, functions, and Pluggables can be added using Java API, and entire toolset can be built into a fully customized distribution. Override `Main` class with your branding, annotate packages with your implementations in your modules, and you're good to go. (Actually, we use custom distro with patented algorithms implemented as Pluggables in production.)

### Build Your Distribution

To build Data Cooker executable FatJAR artifacts, you need Java 17 and Apache Maven.

Minimum supported version of Maven is enforced in the [project file](./pom.xml), so please look into `enforcer` plugin section. For JVM, [Amazon's Corretto](https://corretto.aws/) is the preferred distribution.

There are two profiles to target AWS EMR production environment (`EMR` — selected by default) and for local debugging of ETL processes (`local`), so you have to call
```bash
mvn clean package
```
or
```bash
mvn -Plocal clean package
```
to build a desired flavor of [datacooker-etl.jar](./datacooker-etl-cli/target/datacooker-etl.jar) and [datacooker-dist.jar](./datacooker-dist-cli/target/datacooker-dist.jar).

Currently supported version of EMR is 7.2. For local debugging, Ubuntu 22.04 is recommended (either native or inside WSL).

### Distro Documentation

A companion utility is automatically called in the build process to extract the modules' metadata, and provides the evergreen, always updated distro documentation in a handy HTML format.

By default, distro docs directories are beside executables ([etl/docs](./datacooker-etl-cli/docs/) and [dist/docs](./datacooker-dist-cli/docs/)) where both merged single-file and linked files HTML docs are placed.

### Command Line in General

To familiarize with Data Cooker's command line, just invoke artifact with `-h` as lone argument:
```bash
java -jar datacooker-etl.jar -h
```
or
```bash
java -jar datacooker-dist.jar -h
```

If its output is similar to
```
usage: Data Cooker ETL (ver. 4.5.0)
 -h,--help                  Print full list of command line options and exit
 -s,--script <arg>          Glob patterned path to script files. Mandatory for batch modes
 -H,--highlight             Print HTML of highlighted syntax of passed
                            script to standard output and exit
 -v,--variablesFile <arg>   Path to variables file, name=value pairs per each line
 -V,--variables <arg>       Pass contents of variables file encoded as Base64
 -l,--local                 Run in local batch mode (cluster batch mode otherwise)
 -d,--dry                   -l: Dry run (only check script syntax and
                            print errors to console, if found)
 -m,--driverMemory <arg>    -l: Driver memory, by default Spark uses 1g
 -u,--sparkUI               -l: Enable Spark UI, by default it is disabled
 -L,--localCores <arg>      -l: Set cores #, by default * (all cores)
 -R,--repl                  Run in local mode with interactive REPL
                            interface. Implies -l. -s is optional
 -r,--remoteRepl            Connect to a remote REPL server. -s is optional
 -t,--history <arg>         -R, -r: Set history file location
 -e,--serveRepl             Start REPL server in local or cluster mode. -s is optional
 -i,--host <arg>            Use specified network address:
                            -e: to listen at (default is all)
                            -r: to connect to (in this case, mandatory parameter)
 -p,--port <arg>            -e, -r: Use specified port to listen at or connect to. Default is 9595
```
then everything is OK, working as intended, and you could proceed to begin building your ETL processes.

### ETL Execution Modes

Data Cooker provides a handful of different execution modes, batch and interactive, local and remote, in different combinations.

Refer to the following matrix of command line keys (of ETL tool):

| Execution Mode               | Batch Script \[Dry\] | Interactive... | ...with AutoExec Script \[Dry\] |
|------------------------------|----------------------|----------------|---------------------------------|
| On Spark Cluster             | `-s \[-d\]`          |                |                                 |
| Local                        | `-l -s \[-d\]`       | `-R`           | `-R -s \[-d\]`                  |
| REPL Server On Spark Cluster |                      | `-e`           | `-e -s \[-d\]`                  |
| REPL Server Local            |                      | `-l -e`        | `-l -e -s \[-d\]`               |
| REPL Client                  |                      | `-r`           | `-r -s \[-d\]`                  |

Cells with command line keys indicate which keys to use to run Data Cooker ETL in the desired execution mode. Empty cells indicate unsupported modes.

To specify an ETL Script, use `-s <path/to/script.tdl>` argument. To check just ETL script syntax without performing the actual process, use `-d` switch for a Dry Run in any mode that supports `-s`. If any syntax error is encountered, it'll be reported to console. This path supports glob patterns to load TDL libraries in the specified order.

To specify values for script variables, use either `-v <path/to/vars.properties>` to point to file(s) in Java properties format, or encode that file contents as Base64, and specify it to  `-V <Base64string>` argument.

### Data Cooker Dist

Data Cooker Dist is a companion tool that doesn't support interactive processing, but can be used to efficiently copy data to and from Spark cluster while applying limited transformation on the fly. It has been developed as a replacement of Amazon's `distcp` tool.

General invocation pattern of both tools in the EMR environment is like that:
1. Copy source data from S3 and/or databases to cluster HDFS with Dist
2. Invoke ETL to perform main ETL processing on the cluster
3. Copy result from cluster HDFS to S3 and/or databases with Dist

This usually works much faster than executing ETL with direct calls to external storage (although that is totally possible), because complex processing with Spark may be very IO-heavy, and on-cluster HDFS provides much better data locality and lower latency.

Data Cooker Dist configuration is written in a simple [JSON format](https://pastorgl.github.io/datacooker-etl/JSON.html).

### Local Execution

To run Data Cooker in any of the Local modes, you need an executable artifact built with `local` profile.

You must also use `-l` switch. If you want to limit number of CPU cores available to Spark, use `-L` argument. If you want to change default memory limit of `1G`, use `-m` argument. For example, `-l -L 4 -m 8G`.

If you want to watch for execution of lengthy processing in Spark UI, use `-u` switch to start it up. Otherwise, no Spark UI will be started.

### On-Cluster Execution

If your environment matches with `EMR` profile, you may take artifact built with that profile, and use your favorite Spark submitter to pass it to cluster, and invoke with `-s` and  `-v` or `-V` command line switches. Entry class name is `io.github.pastorgl.datacooker.cli.Main`.

Otherwise, you may first need to tinker with [datacooker-commons](./datacooker-commons/pom.xml) and both [datacooker-etl-cli](./datacooker-etl-cli/pom.xml) and [datacooker-dist-cli](./datacooker-dist-cli/pom.xml) project manifests and adjust library versions to match your environment. Because there are no exactly same Spark setups in the production, that would be necessary in most cases.

We recommend to wrap submitter calls with some scripting and automate execution with a CI/CD service.

### REPL Modes

In addition to standard batch modes, which just execute a single TDL Script and then exit, there are interactive modes with REPL, useful if you want to interactively debug your processes.

To run in the Local REPL mode, use `-R` switch. If `-s` were specified, this Script becomes AutoExec, which will be executed before displaying the REPL prompt.

To run just a REPL Server (either Local with `-l` or On-Cluster otherwise), use `-e` switch. If `-s` were specified, this Script becomes AutoExec, which will be executed before starting the REST service. `-i` and `-p` control which interface and port to use for REST. By default, configuration is `0.0.0.0:9595`.

To run a REPl Client only, use `-r`. You need to provide which Server is to connect to using `-i` and `-p`. By default, configuration is `localhost:9595`.

Please note that currently protocol between Server and Client is simple HTTP without security and authentication. If you intend to use it within production environment, you should wrap it into the secure tunnel and use some sort of authenticating proxy.

By default, REPL shell stores command history in your home directory, but if you want to redirect some session history to a different location, use `-t <path/to/file>` switch.

After starting up, you may see some Spark logs, and then the following prompt:

```
=============================================
Data Cooker ETL REPL interactive (ver. 5.0.0)
Type TDL statements to be executed in the REPL context in order of input, or a command.
Statement must always end with a semicolon. If not, it'll be continued on a next line.
If you want to type several statements at once on several lines, end each line with \
Type \QUIT; to end session and \HELP; for list of all REPL commands and shortcuts
datacooker> _         
```

Follow the instructions and explore available `\COMMAND`s with the `\HELP COMMAND;` command.

You may freely execute any valid TDL statements, view your data, load scripts from files, and even record them directly in REPL.

Also, you may use some familiar shell shortcuts (like reverse search with `Ctrl+R`, automatic last commands expansion with `!n`) and contextual auto-complete of TDL statements with `TAB` key.

Regarding Spark logs, in REPL shell they're automatically set to `WARN` level. If you want to switch back to default `INFO` level, use

```sql
OPTIONS @log_level='INFO';
```

## Examples

### Perform daily CSV ingest
...with sanitization, and split data into different subdirectories by `userid` column's first letter.

Source data is stored on S3 in CSV format, result goes to another bucket as compressed Parquet files. 
```sql
-- most variables come from command line, but we specify defaults (for local env) where it makes sense
LET $p_p_l = $p_p_l DEFAULT 20; -- parts per letter
LET $parts = 16 * $p_p_l; -- letters are 16 hex digits
LET $tz = $tz DEFAULT 'Europe/London'; -- default country is UK
-- by default, trash all columns after ts (there can be many)
LET $input_columns = $input_columns DEFAULT ["userid", "accuracy", "lat", "lon", "ts", _];

-- that's the source
CREATE "source" textColumnar(@delimiter = ',')
    COLUMNS ($input_columns)
    FROM 's3://ingress-bucket/{$year}/{$month}/{$day}/*.csv'
    PARTITION $parts;

-- sanitize, process timestamps and coords
SELECT "userid", "lat", "lon", "ts",
    DT_FORMAT($tz, 'yyyy''-''MM''-''dd', "ts") AS "date",
    DT_YEAR($tz, "ts") AS "year",
    DT_MONTH($tz, "ts") AS "month",
    DT_DAY($tz, "ts") AS "day",
    DT_DOW($tz, "ts") AS "dow",
    DT_HOUR($tz, "ts") AS "hour",
    H3(9, "lat", "lon") AS "gid_9",
    H3(10, "lat", "lon") AS "gid_10"
    FROM "source"
    INTO "processed"
    WHERE ("userid" != 'maid') AND -- source CSV may contain header line
        (("accuracy" LIKE '.+') -- there can be empty accuracy; accept only less than 50m; coords may be bad
            ? (("accuracy" < 50) AND ("lat" BETWEEN -90 AND 90 AND "lon" BETWEEN -180 AND 180))
            : FALSE
        );

-- shuffle records. partition range defined by first letter of "userid" 
ALTER "processed" KEY INT ('0x' || STR_SLICE("userid", 0, 1)) * $p_p_l + RANDOM $p_p_l PARTITION $parts;

-- for each letter select only needed partitions
LET $hex_digits = STR_SPLIT('0123456789abcdef', '', 16);
LOOP $digit IN $hex_digits BEGIN
    LET $range = ARR_RANGE(INT ('0x' || $digit) * $p_p_l, INT ('0x' || $digit) * $p_p_l + $p_p_l - 1);

    SELECT *
        FROM "processed" PARTITION $range
        INTO "timezoned/{$digit}";
END;

-- write $p_p_l parquet files in each letter subdir 
COPY "timezoned/" * columnarParquet(@codec = 'SNAPPY')
    INTO 's3://timezoned-bucket/{$year}/{$month}/{$day}/';
```

### Copy some data for further processing
...this time with Dist.

Source data is on the foreign S3, we're copying to Cluster's HDFS as raw text files (no transformation).
```json
{
    "s3toHdfs" : [
        {
            "name": "rawfiles",
            "source": {
                "adapter" : "s3directText",
                "path" : "s3d://foreign-bucket/our-datapath/*",
                "part_count" : 1000,
                "params": {
                    "access_key" : "...",
                    "secret_key" : "...",
                    "endpoint" : "s3.datasink.provider.com",
                    "region" : "eu-1"
                }
            },
            "dest": {
                "adapter" : "hadoopText",
                "path" : "hdfs:///input/"
            }
        }
    ]
}
```

This is fully equivalent to Data Cooker ETL's SQL (which is more concise):
```sql
CREATE "rawfiles" s3directText(@access_key = '...', @secret_key = '...', @endpoint = 's3.datasink.provider.com', @region = 'eu-1')
    FROM 's3d://foreign-bucket/our-datapath/*'
    PARTITION 1000;

COPY "rawfiles" hadoopText
    INTO 'hdfs:///input/';
```

But because JSON is a machine-friendly format, Dist configurations can be automatically generated by higher-level code if needed. In case of simpler data pipelines, it can be a substantial benefit to have more machine-oriented tool in the toolset beside the human-oriented one.

### Some complex ETL
Not really complex, but we're gathering small table from a database, and user/password are passed via environment variables.

Then, we pass it to a Pluggable that calculates statistical indicator of 'parametric scores' for each of user's top 10 residency postcodes, according to each hour's multiplier.
```sql
CREATE "mults" jdbcColumnar(
       @driver = 'org.postgresql.Driver',
       @url = 'jdbc:postgresql://pg.dbcluster.local/GLOBAL',
       @user = '{$"ENV:DB_USER"}',
       @password = '{$"ENV:DB_SECRET"}'
    )   
    FROM 'SELECT hour, mult FROM hourly_multipliers WHERE 1 = ? + ?' -- one partition, so ? ? are 0 and 1
    PARTITION 1;

ALTER "mults" KEY "hour";

CREATE "residents" textColumnar
       VALUE ("userid", "hour", "postcode", _)
       FROM 'hdfs:///input/residents/*/*.tsv'
       PARTITION 400;

CALL parametricScore(
        @top_scores = 10,
        @grouping_attr = "userid",
        @value_attr = "postcode",
        @count_attr = "hour",
        @multiplier_attr = "mult"
    )
    INPUT "values" FROM "residents", "multipliers" FROM "mults"
    OUTPUT "scores";

COPY "scores" hadoopText() INTO 'hdfs:///output/scores';
```

### Create a Custom Function
Well, its name speaks for itself.

```sql
CREATE FUNCTION daysPerMonth(@year, @month) AS BEGIN
   IF $month IN [4,6,9,11] THEN
      RETURN 30;
   END;
   
   IF $month == 2 THEN
      IF ($year % 400 == 0) || ($year % 4 == 0) && ($year % 100 <> 0) THEN
         RETURN 29;
      ELSE
         RETURN 28;
      END;
   END;
   
   RETURN 31;
END;
```
