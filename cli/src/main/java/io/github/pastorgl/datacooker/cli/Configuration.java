/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.cli;

import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.scripting.ScriptHolder;
import org.apache.commons.cli.*;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.StringReader;
import java.util.Base64;
import java.util.ListIterator;
import java.util.Properties;

public class Configuration {
    protected Options options;
    protected CommandLine commandLine;

    public Configuration() {
        options = new Options();

        addOption("h", "help", false, "Print a list of command line options and exit");
        addOption("s", "script", true, "TDL4 script file");
        addOption("V", "variables", true, "name=value pairs of substitution variables for the Spark config encoded as Base64");
        addOption("v", "variablesFile", true, "Path to variables file, name=value pairs per each line");
        addOption("l", "local", false, "Run in local mode (its options have no effect otherwise)");
        addOption("m", "driverMemory", true, "Driver memory for local mode, by default Spark uses 1g");
        addOption("u", "sparkUI", false, "Enable Spark UI for local mode, by default it is disabled");
        addOption("L", "localCores", true, "Set cores # for local mode, by default * -- all cores");
        addOption("d", "dry", false, "Dry run: just check script syntax and print errors to console, if found");
    }

    public ScriptHolder build(JavaSparkContext context) throws Exception {
        Properties variables = new Properties();

        String variablesSource = null;
        if (hasOption("V")) {
            variablesSource = new String(Base64.getDecoder().decode(getOptionValue("V")));
        } else if (hasOption("v")) {
            String variablesFile = getOptionValue("v");

            Path sourcePath = new Path(variablesFile);
            String qualifiedPath = sourcePath.getFileSystem(context.hadoopConfiguration()).makeQualified(sourcePath).toString();

            int lastSlash = variablesFile.lastIndexOf('/');
            variablesFile = (lastSlash < 0) ? variablesFile : variablesFile.substring(0, lastSlash);

            variablesSource = context.wholeTextFiles(variablesFile)
                    .filter(t -> t._1.equals(qualifiedPath))
                    .map(Tuple2::_2)
                    .first();
        }
        if (variablesSource != null) {
            variables.load(new StringReader(variablesSource));
        }

        String script;
        if (hasOption("s")) {
            String sourceFile = getOptionValue("script");

            Path sourcePath = new Path(sourceFile);
            String qualifiedPath = sourcePath.getFileSystem(context.hadoopConfiguration()).makeQualified(sourcePath).toString();

            int lastSlash = sourceFile.lastIndexOf('/');
            sourceFile = (lastSlash < 0) ? sourceFile : sourceFile.substring(0, lastSlash);

            script = context.wholeTextFiles(sourceFile)
                    .filter(t -> t._1.equals(qualifiedPath))
                    .map(Tuple2::_2)
                    .first();
        } else {
            throw new InvalidConfigurationException("TDL4 script wasn't supplied. There is nothing to run");
        }

        return new ScriptHolder(script, variables);
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

    public void setCommandLine(String[] args, String utility) throws ParseException {
        commandLine = new BasicParser() {
            @Override
            protected void processOption(String arg, ListIterator iter) throws ParseException {
                if (getOptions().hasOption(arg)) {
                    super.processOption(arg, iter);
                }
            }
        }.parse(options, args);

        if (commandLine.hasOption("help")) {
            printHelp(utility);

            System.exit(0);
        }
    }

    public void printHelp(String utility) {
        new HelpFormatter().printHelp(utility, options);
    }
}
