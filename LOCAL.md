### Local testing How-to

To locally test Data Cooker ETL, you need an executable artifact [built](./BUILD.md) with `local` profile.

First, invoke it with `--help` argument to get a list of options:
```bash
java -jar datacooker-etl-cli.jar --help
```

If its output is similar to
```
usage: Data Cooker Command Line Interface
-d,--dry                   Dry run: just check script syntax and print
                           errors to console, if found
-h,--help                  Print a list of command line options and exit
-l,--local                 Run in local mode (its options have no effect
                           otherwise)
-L,--localCores <arg>      Set cores # for local mode, by default * --
                           all cores
-m,--driverMemory <arg>    Driver memory for local mode, by default Spark
                           uses 1g
-s,--script <arg>          TDL4 script file
-u,--sparkUI               Enable Spark UI for local mode, by default it
                           is disabled
-V,--variables <arg>       name=value pairs of substitution variables for
                           the Spark config encoded as Base64
-v,--variablesFile <arg>   Path to variables file, name=value pairs per
                           each line
```
then everything is OK, working as intended, and you could proceed to test your ETLs.

To specify ETL script, use `--script <path/to/script.tdl>` argument. To check just ETL script syntax without performing the actual process, use `--dry` switch.

To specify values for script variables, use either `--variablesFile <path/to/variables.ini>` with pairs of `name=value` of each variable separated by newline, or encode that file contents as Base64, and specify it to  `--variables <Base64string>` argument.

You must also use `--local` switch. If you want to limit number of CPU cores available to Spark, use `--localCores` argument. If you want to change default memory limit of 1G, use `--driverMemory` argument. For example, `-l -L 4 -m 8G`.