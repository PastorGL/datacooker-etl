### Execution Modes

**Data Cooker ETL** provides a handful of different execution modes, batch and interactive, local and remote, in
different combinations.

Refer to following matrix:

 Execution Mode               | Batch Script \[Dry\] | Interactive... | ...with AutoExec Script \[Dry\] 
------------------------------|----------------------|----------------|---------------------------------
 On Spark Cluster             | -s \[-d\]            |                |
 Local                        | -l -s \[-d\]         | -R             | -R -s \[-d\]                    
 REPL Server On Spark Cluster |                      | -e             | -e -s \[-d\]                    
 REPL Server Local            |                      | -l -e          | -l -e -s \[-d\]                 
 REPL Client                  |                      | -r             | -r -s \[-d\]                    

Cells with command line keys indicate which keys to use to run Data Cooker ETL in the desired execution mode. Empty
cells indicate unsupported modes.

### Command Line in General

To familiarize with CLI command line, just invoke artifact with `-h` as lone argument:

```bash
java -jar datacooker-etl-cli.jar -h
```

If its output is similar to

```
usage: Data Cooker ETL (ver. 3.8.0)
 -h,--help                  Print full list of command line options and
                            exit
 -s,--script <arg>          TDL4 script file. Mandatory for batch modes
 -v,--variablesFile <arg>   Path to variables file, name=value pairs per
                            each line
 -V,--variables <arg>       Pass contents of variables file encoded as
                            Base64
 -l,--local                 Run in local batch mode (cluster batch mode
                            otherwise)
 -d,--dry                   -l: Dry run (only check script syntax and
                            print errors to console, if found)
 -m,--driverMemory <arg>    -l: Driver memory, by default Spark uses 1g
 -u,--sparkUI               -l: Enable Spark UI, by default it is disabled
 -L,--localCores <arg>      -l: Set cores #, by default * (all cores)
 -R,--repl                  Run in local mode with interactive REPL
                            interface. Implies -l. -s is optional
 -r,--remoteRepl            Connect to a remote REPL server. -s is optional
 -t,--history               -R, -r: Set history file location
 -i,--host <arg>            Use specified network address:
                            -e: to listen at (default is all)
                            -r: to connect to (in this case, mandatory
                            parameter)
 -e,--serveRepl             Start REPL server in local or cluster mode. -s
                            is optional
 -p,--port <arg>            -e, -r: Use specified port to listen at or
                            connect to. Default is 9595
```

then everything is OK, working as intended, and you could proceed to begin building your ETL processes.

To specify an ETL Script, use `-s <path/to/script.tdl>` argument. To check just ETL script syntax without performing
the actual process, use `-d` switch for a Dry Run in any mode that supports `-s`. If any syntax error is encountered,
it'll be reported to console.

To specify values for script variables, use either `-v <path/to/vars.properties>` to point to file in Java
properties format, or encode that file contents as Base64, and specify it to  `-V <Base64string>` argument.

### Local Execution

To run Data Cooker ETL in any of the Local modes, you need an executable artifact [built](BUILD.md) with `local`
profile.

You must also use `-l` switch. If you want to limit number of CPU cores available to Spark, use `-L`
argument. If you want to change default memory limit of `1G`, use `-m` argument. For example, `-l -L 4 -m 8G`.

If you want to watch for execution of lengthy processing in Spark UI, use `-u` switch to start it up. Otherwise, no
Spark UI will be started.

### On-Cluster Execution

If your environment matches with `EMR` profile (which is targeted to EMR 6.9 with Java 11), you may take
artifact [built](BUILD.md) with that profile, and use your favorite Spark submitter to pass it to cluster, and invoke
with `-s` and  `-v` or `-V` command line switches. Entry class name is `io.github.pastorgl.datacooker.cli.Main`.

Otherwise, you may first need to tinker with [commons](./commons/pom.xml) and [cli](./cli/pom.xml) project manifests and
adjust library versions to match your environment. Because there are no exactly same Spark setups in the production,
that would be necessary in most cases.

We recommend to wrap submitter calls with some scripting and automate execution with CI/CD service.

### REPL Modes

In addition to standard batch modes, which just execute a single TDL4 Script and then exit, there are interactive modes
with REPL, useful if you want to interactively debug your processes.

To run in the Local REPL mode, use `-R` switch. If `-s` were specified, this Script becomes AutoExec, which will be
executed before displaying the REPL prompt.

To run just a REPL Server (either Local with `-l` or On-Cluster otherwise), use `-e` switch. If `-s` were specified,
this Script becomes AutoExec, which will be executed before starting the REST service. `-i` and `-p` control which
interface and port to use for REST. By default, configuration is `0.0.0.0:9595`.

To run a REPl Client only, use `-r`. You need to provide which Server is to connect to using `-i` and `-p`. By default,
configuration is `localhost:9595`.

Please note that currently protocol between Server and Client is simple HTTP without security and authentication. If you
intend to use it within production environment, you should wrap it into the secure tunnel and use some sort of
authenticating proxy.

By default, REPL shell stores command history in your home directory, but if you want to redirect some session history
to a different location, use `-t <path/to/file>` switch.

After starting up, you may see some Spark logs, and then the following prompt:

```
=============================================
Data Cooker ETL REPL interactive (ver. 3.8.0)
Type TDL4 statements to be executed in the REPL context in order of input, or a command.
Statement must always end with a semicolon. If not, it'll be continued on a next line.
If you want to type several statements at once on several lines, end each line with \
Type \QUIT; to end session and \HELP; for list of all REPL commands and shortcuts
datacooker> _         
```

Follow the instructions and explore available `\COMMAND`s with the `\HELP COMMAND;` command.

You may freely execute any valid TDL4 statements, view your data, load scripts from files, and even record them
directly in REPL.

Also, you may use some familiar shell shortcuts (like reverse search with `Ctrl+R`, automatic last commands expansion
with `!n`) and contextual auto-complete of TDL4 statements with `TAB` key.

Regarding Spark logs, in REPL shell they're automatically set to `WARN` level. If you want to switch back to default
`INFO` level, use

```sql
OPTIONS @log_level='INFO';
```
