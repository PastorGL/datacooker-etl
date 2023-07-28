### Local Execution

To locally test Data Cooker ETL, you need an executable artifact [built](BUILD.md) with `local` profile.

First, invoke it with `--help` argument to get a list of options:
```bash
java -jar datacooker-etl-cli.jar --help
```

If its output is similar to
```
usage: Data Cooker ETL
 -h,--help                  Print a list of command line options and exit
 -s,--script <arg>          TDL4 script file
 -d,--dry                   Dry run: only check script syntax and print
                            errors to console, if found
 -v,--variablesFile <arg>   Path to variables file, name=value pairs per
                            each line
 -V,--variables <arg>       Pass contents of variables file encoded as
                            Base64
 -l,--local                 Run in local mode (its options have no effect
                            otherwise)
 -m,--driverMemory <arg>    -l: Driver memory, by default Spark uses 1g
 -u,--sparkUI               -l: Enable Spark UI, by default it is disabled
 -L,--localCores <arg>      -l: Set cores #, by default * (all cores)
 -R,--repl                  Run in local mode with interactive REPL
                            interface. -s is optional
 -i,--history <arg>         -R: Set history file location
 ```
then everything is OK, working as intended, and you could proceed to test your ETLs (you may safely ignore Spark warnings, if there are any).

To specify ETL script, use `--script <path/to/script.tdl>` argument. To check just ETL script syntax without performing the actual process, use `--dry` switch.

To specify values for script variables, use either `--variablesFile <path/to/vars.properties>` to point to file in Java properties format, or encode that file contents as Base64, and specify it to  `--variables <Base64string>` argument.

You must also use `--local` switch. If you want to limit number of CPU cores available to Spark, use `--localCores` argument. If you want to change default memory limit of 1G, use `--driverMemory` argument. For example, `-l -L 4 -m 8G`.

### REPL Mode

In addition to standard local mode, which just executes a single TDL4 script and then exits, there is a local REPL mode, useful if you want to interactively debug some scripts.

To run in REPL mode, use `--repl` switch. By default, it stores command history in your home directory, but if you want to redirect some session history to a different location, use `--history <path/to/file>` switch.

After starting up, you'll see some Spark logs, and then the following prompt
```
================================
Data Cooker ETL REPL interactive
Type TDL4 statements to be executed in the REPL context in order of input, or a command.
Statement must always end with a semicolon. If not, it'll be continued on a next line.
If you want to type several statements at once on several lines, end each line with \
Type \QUIT; to end session and \HELP; for list of all REPL commands
datacooker> _         
```

Follow the instructions and explore `\COMMANDs` with `\HELP \COMMAND;` command.

After that, you may freely execute any TDL4 statements, load scripts from files, and even record them directly in REPL.

Regarding Spark logs, they're automatically set to `WARN` level. If you want to switch to default `INFO`, use
```sql
OPTIONS @log_level='INFO';
```

### Cluster Execution

If your environment matches with `EMR` profile (which is targeted to EMR 6.9 with Java 11), you may take artifact [built](BUILD.md) with that profile, and use your favorite Spark submitter to pass it to cluster, and invoke with `--script` and  `-v` or `-V` command line switches. Entry class name is `io.github.pastorgl.datacooker.cli.Main`.

Otherwise, you may first need to tinker with [commons](./commons/pom.xml) and [cli](./cli/pom.xml) project manifests and adjust library versions to match your environment. Because there are no exactly same Spark setups in the production, that would be necessary in most cases.

We recommend to wrap submitter calls with some scripting and automate execution with CI/CD service.