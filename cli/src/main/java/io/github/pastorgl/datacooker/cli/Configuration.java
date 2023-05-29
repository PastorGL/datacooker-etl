/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.cli;

import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.scripting.VariablesContext;
import org.apache.commons.cli.*;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.StringReader;
import java.util.*;

public class Configuration {
    protected Options options;
    protected CommandLine commandLine;

    private HelpFormatter hf = new HelpFormatter();

    public Configuration() {
        options = new Options();
        hf.setOptionComparator(null);

        addOption("h", "help", false, "Print a list of command line options and exit");
        addOption("s", "script", true, "TDL4 script file");
        addOption("d", "dry", false, "Dry run: only check script syntax and print errors to console, if found");
        addOption("v", "variablesFile", true, "Path to variables file, name=value pairs per each line");
        addOption("V", "variables", true, "Pass contents of variables file encoded as Base64");
        addOption("l", "local", false, "Run in local mode (its options have no effect otherwise)");
        addOption("m", "driverMemory", true, "-l: Driver memory, by default Spark uses 1g");
        addOption("u", "sparkUI", false, "-l: Enable Spark UI, by default it is disabled");
        addOption("L", "localCores", true, "-l: Set cores #, by default * (all cores)");
        addOption("R", "repl", false, "Run in local mode with interactive REPL interface. -s is optional");
        addOption("i", "history", true, "-R: Set history file location");
    }

    public VariablesContext variables(JavaSparkContext context) throws Exception {
        StringBuilder variablesSource = new StringBuilder();
        if (hasOption("v")) {
            String variablesFile = getOptionValue("v");

            Path sourcePath = new Path(variablesFile);
            String qualifiedPath = sourcePath.getFileSystem(context.hadoopConfiguration()).makeQualified(sourcePath).toString();

            int lastSlash = variablesFile.lastIndexOf('/');
            variablesFile = (lastSlash < 0) ? variablesFile : variablesFile.substring(0, lastSlash);

            variablesSource.append(context.wholeTextFiles(variablesFile)
                    .filter(t -> t._1.equals(qualifiedPath))
                    .map(Tuple2::_2)
                    .first());
        }
        if (hasOption("V")) {
            variablesSource.append("\n");
            variablesSource.append(new String(Base64.getDecoder().decode(getOptionValue("V"))));
        }

        Properties properties = new Properties();
        if (variablesSource.length() > 0) {
            properties.load(new StringReader(variablesSource.toString()));
        }

        Map<String, Object> variables = new HashMap<>();
        for (Map.Entry e : properties.entrySet()) {
            String key = String.valueOf(e.getKey());
            Object v = e.getValue();
            String value = String.valueOf(v);

            int last = value.length() - 1;
            if ((value.indexOf('[') == 0) && (value.lastIndexOf(']') == last)) {
                value = value.substring(1, last);

                if (value.contains("'")) {
                    boolean inString = false;
                    List<String> strings = new ArrayList<>();
                    StringBuilder cur = null;
                    for (int i = 0, len = value.length(); i < len; i++) {
                        char c = value.charAt(i);
                        if (inString) {
                            if (c != '\'') {
                                cur.append(c);
                            } else { // c == '
                                if ((i + 1) < len) {
                                    if (value.charAt(i + 1) != '\'') {
                                        inString = false;
                                        strings.add(cur.toString());
                                    } else {
                                        cur.append("'");
                                        i++;
                                    }
                                } else {
                                    strings.add(cur.toString());
                                }
                            }
                        } else {
                            if (c == '\'') {
                                inString = true;
                                cur = new StringBuilder();
                            }
                        }
                    }

                    v = strings.toArray();
                } else {
                    String[] vv = value.split(",");
                    v = Arrays.stream(vv).map(vvv -> Double.parseDouble(vvv.trim())).toArray();
                }
            } else if ((value.indexOf('\'') == 0) && (value.lastIndexOf('\'') == last)) {
                v = value.substring(1, last);
            }
            variables.put(key, v);
        }

        VariablesContext variablesContext = new VariablesContext();
        variablesContext.putAll(variables);
        return variablesContext;
    }

    public String script(JavaSparkContext context, String sourceFile) {
        try {
            Path sourcePath = new Path(sourceFile);
            String qualifiedPath = sourcePath.getFileSystem(context.hadoopConfiguration()).makeQualified(sourcePath).toString();

            int lastSlash = sourceFile.lastIndexOf('/');
            sourceFile = (lastSlash < 0) ? sourceFile : sourceFile.substring(0, lastSlash);

            return context.wholeTextFiles(sourceFile)
                    .filter(t -> t._1.equals(qualifiedPath))
                    .map(Tuple2::_2)
                    .first();
        } catch (Exception e) {
            throw new InvalidConfigurationException("Error while reading TDL4 script file");
        }
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
        hf.printHelp(utility, options);
    }
}