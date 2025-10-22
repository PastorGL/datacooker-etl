/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.cli;

import com.google.common.io.Resources;
import io.github.pastorgl.datacooker.DataCooker;
import io.github.pastorgl.datacooker.cli.repl.local.Local;
import io.github.pastorgl.datacooker.cli.repl.remote.Client;
import io.github.pastorgl.datacooker.data.DataContext;
import io.github.pastorgl.datacooker.rest.Server;
import io.github.pastorgl.datacooker.scripting.*;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.nio.charset.StandardCharsets;

public class Main {
    public static final Logger LOG = Logger.getLogger(Main.class);

    private static final String ver = Helper.getVersion();

    protected String getExeName() {
        return "Data Cooker ETL";
    }

    protected String getReplPrompt() {
        return "datacooker";
    }

    public static void main(String[] args) {
        new Main().run(args);
    }

    public Main() {
        Helper.exportAllToAll();
    }

    public void run(String[] args) {
        Configuration config = new Configuration();

        JavaSparkContext context = null;
        try {
            config.setCommandLine(args);

            if (config.hasOption("help")) {
                config.printHelp(getExeName(), ver);

                System.exit(0);
            }

            boolean remote = false;
            boolean serve = false;
            boolean repl = false;
            boolean local = config.hasOption("local");
            if (config.hasOption("highlight")) {
                if (!config.hasOption("script")) {
                    throw new RuntimeException("No script to highlight was specified");
                }

                local = true;
            } else {
                remote = config.hasOption("remoteRepl");
                serve = config.hasOption("serveRepl");
                repl = config.hasOption("repl");
            }

            if ((remote && serve) || (remote && repl) || (serve && repl)) {
                throw new RuntimeException("Local interactive REPL, REPL server, and connect to remote REPL modes are mutually exclusive");
            }

            if (remote) {
                var defaultLogProps = "org/apache/spark/log4j2-defaults.properties";
                var url = Main.class.getClassLoader().getResource(defaultLogProps);
                LoggerContext loggerContext = (LoggerContext) LogManager.getContext(false);
                loggerContext.setConfigLocation(url.toURI());
                loggerContext.start();

                new Client(config, getExeName(), ver, getReplPrompt()).loop();
            } else {
                if (!repl && !serve && !config.hasOption("script")) {
                    throw new RuntimeException("No script to execute in the batch mode was specified");
                }

                SparkConf sparkConf = new SparkConf()
                        .setAppName(getExeName())
                        .set("spark.serializer", org.apache.spark.serializer.KryoSerializer.class.getCanonicalName());

                if (repl || local) {
                    String cores = "*";
                    if (config.hasOption("localCores")) {
                        cores = config.getOptionValue("localCores");
                    }

                    sparkConf
                            .setMaster("local[" + cores + "]")
                            .set("spark.network.timeout", "10000")
                            .set("spark.ui.enabled", String.valueOf(config.hasOption("sparkUI")));

                    if (!serve) {
                        sparkConf.set("spark.log.level", "WARN");
                    }

                    if (config.hasOption("driverMemory")) {
                        sparkConf.set("spark.driver.memory", config.getOptionValue("driverMemory"));
                    }
                }

                context = new JavaSparkContext(sparkConf);
                context.hadoopConfiguration().set(FileInputFormat.INPUT_DIR_RECURSIVE, Boolean.TRUE.toString());

                DataCooker.initialize(context, new OptionsContext(), new DataContext(), VariablesContext.createGlobal());
                Helper.populateVariables(config, context);

                if (repl) {
                    new Local(config, getExeName(), ver, getReplPrompt(), context).loop();
                } else {
                    if (config.hasOption("highlight")) {
                        try {
                            String header = Resources.toString(Resources.getResource("hl-h.htm"), StandardCharsets.UTF_8).replace("%title%", config.getOptionValue("script"));
                            String footer = Resources.toString(Resources.getResource("hl-f.htm"), StandardCharsets.UTF_8);

                            String script = Helper.loadScript(config.getOptionValue("script"), context);

                            System.out.print(header + new Highlighter(script).highlight() + footer);
                        } catch (Exception e) {
                            LOG.error(e.getMessage(), e);

                            System.exit(9);
                        }
                    } else {
                        if (config.hasOption("script")) {
                            new BatchRunner(config, context).run();
                        }
                    }

                    if (serve) {
                        new Server(config, ver).serve();

                        context = null;
                    }
                }
            }
        } catch (Exception ex) {
            if (ex instanceof RaiseException) {
                LOG.error(ex.getMessage(), ex);
                System.exit(10);
            }

            if (ex instanceof ParseException) {
                config.printHelp(getExeName(), ver);
            } else {
                LOG.error(ex.getMessage(), ex);
            }

            System.exit(1);
        } finally {
            if (context != null) {
                context.stop();
            }
        }
    }
}
