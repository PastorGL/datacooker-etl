/**
 * Copyright (C) 2023 Data Cooker team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.dist;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.cli.*;

import java.io.IOException;
import java.io.Reader;
import java.util.Collection;
import java.util.HashMap;
import java.util.ListIterator;
import java.util.Map;

public class Configuration {
    protected Options options;
    protected CommandLine commandLine;

    public Configuration() {
        options = new Options();

        addOption("c", "config", true, "Path to configuration file in JSON format)");
        addOption("t", "tmpDir", true, "Location for temporary files");
        addOption("d", "direction", true, "Copy direction, if configuration file contains more than one directions");
        addOption("h", "help", false, "Print a list of command line options and exit");
        addOption("l", "local", false, "Run in local mode (its options have no effect otherwise)");
        addOption("m", "driverMemory", true, "Driver memory for local mode, by default Spark uses 1g");
        addOption("u", "sparkUI", false, "Enable Spark UI for local mode, by default it is disabled");
        addOption("L", "localCores", true, "Set cores # for local mode, by default * -- all cores");
    }

    private Map<String, DistTask[]> copyTasks;

    public void read(Reader reader) throws IOException {
        ObjectMapper om = new ObjectMapper();
        om.enable(DeserializationFeature.USE_JAVA_ARRAY_FOR_JSON_ARRAY);
        copyTasks = om.readValue(reader, new TypeReference<Map<String, DistTask[]>>() {
        });
    }

    public DistTask[] getDirection(String direction) {
        return copyTasks.get(direction);
    }

    public Collection<String> directions() {
        return copyTasks.keySet();
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

    static public class DistTask {
        @JsonProperty(required = true)
        public DistLocation source;
        @JsonProperty(required = true)
        public DistLocation dest;
    }

    static public class DistLocation {
        @JsonProperty(required = true)
        public String adapter;
        @JsonProperty(required = true)
        public String path;
        @JsonSetter(nulls = Nulls.SKIP)
        public Map<String, Object> params = new HashMap<>();
    }
}
