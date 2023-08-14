/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.cli;

import org.apache.commons.cli.*;

import java.util.ListIterator;

public class Configuration {
    protected Options options;
    protected CommandLine commandLine;

    private final HelpFormatter hf = new HelpFormatter();

    public Configuration() {
        options = new Options();
        hf.setOptionComparator(null);

        addOption("h", "help", false, "Print full list of command line options and exit");
        addOption("s", "script", true, "TDL4 script file. Mandatory for batch modes");
        addOption("v", "variablesFile", true, "Path to variables file, name=value pairs per each line");
        addOption("V", "variables", true, "Pass contents of variables file encoded as Base64");
        addOption("l", "local", false, "Run in local batch mode (cluster batch mode otherwise)");
        addOption("d", "dry", false, "-l: Dry run (only check script syntax and print errors to console, if found)");
        addOption("m", "driverMemory", true, "-l: Driver memory, by default Spark uses 1g");
        addOption("u", "sparkUI", false, "-l: Enable Spark UI, by default it is disabled");
        addOption("L", "localCores", true, "-l: Set cores #, by default * (all cores)");
        addOption("R", "repl", false, "Run in local mode with interactive REPL interface. Implies -l. -s is optional");
        addOption("r", "remoteRepl", false, "Connect to a remote REPL server. -s is optional");
        addOption("t", "history", true, "-R, -r: Set history file location");
        addOption("e", "serveRepl", false, "Start REPL server in local or cluster mode. -s is optional");
        addOption("i", "host", true, "Use specified network address:\n" +
                "-e: to listen at (default is all)\n" +
                "-r: to connect to (in this case, mandatory parameter)");
        addOption("p", "port", true, "-e, -r: Use specified port to listen at or connect to. Default is 9595");
    }

    public void addOption(String opt, String longOpt, boolean hasArg, String description) {
        options.addOption(opt, longOpt, hasArg, description);
    }

    public String getOptionValue(String opt) {
        return commandLine.getOptionValue(opt);
    }

    public boolean hasOption(String opt) {
        return commandLine.hasOption(opt);
    }

    public void setCommandLine(String[] args) throws ParseException {
        commandLine = new BasicParser() {
            @Override
            protected void processOption(String arg, ListIterator iter) throws ParseException {
                if (getOptions().hasOption(arg)) {
                    super.processOption(arg, iter);
                }
            }
        }.parse(options, args);
    }

    public void printHelp(String utility, String version) {
        hf.printHelp(utility + " (ver. " + version + ")", options);
    }
}