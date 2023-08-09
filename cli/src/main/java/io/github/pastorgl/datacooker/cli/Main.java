/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.cli;

import io.github.pastorgl.datacooker.cli.repl.local.Local;
import io.github.pastorgl.datacooker.cli.repl.remote.Client;
import io.github.pastorgl.datacooker.rest.Server;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.net.URL;
import java.util.jar.Manifest;

public class Main {
    public static final Logger LOG = Logger.getLogger(Main.class);

    private static String ver;

    protected static String getVersion() {
        if (ver == null) {
            try {
                URL url = Main.class.getClassLoader().getResource("META-INF/MANIFEST.MF");
                Manifest man = new Manifest(url.openStream());

                ver = man.getMainAttributes().getValue("Implementation-Version");
            } catch (Exception ignore) {
            }
            if (ver == null) {
                ver = "unknown";
            }
        }

        return ver;
    }

    protected String getExeName() {
        return "Data Cooker ETL";
    }

    protected String getReplPrompt() {
        return "datacooker";
    }

    public static void main(String[] args) {
        new Main().run(args);
    }

    public void run(String[] args) {
        Configuration config = new Configuration();

        JavaSparkContext context = null;
        try {
            config.setCommandLine(args);

            if (config.hasOption("help")) {
                config.printHelp(getExeName(), getVersion());

                System.exit(0);
            }

            boolean remote = config.hasOption("remoteRepl");
            boolean serve = config.hasOption("serveRepl");
            boolean repl = config.hasOption("repl");

            if ((remote && serve) || (remote && repl) || (serve && repl)) {
                throw new RuntimeException("Local interactive REPL, REPL server, and connect to remote REPL modes are mutually exclusive");
            }

            SparkConf sparkConf = new SparkConf()
                    .setAppName(getExeName())
                    .set("spark.serializer", org.apache.spark.serializer.KryoSerializer.class.getCanonicalName());

            boolean local = repl || config.hasOption("local");
            if (local) {
                String cores = "*";
                if (config.hasOption("localCores")) {
                    cores = config.getOptionValue("localCores");
                }

                sparkConf
                        .setMaster("local[" + cores + "]")
                        .set("spark.network.timeout", "10000")
                        .set("spark.ui.enabled", String.valueOf(config.hasOption("sparkUI")));

                if (config.hasOption("driverMemory")) {
                    sparkConf.set("spark.driver.memory", config.getOptionValue("driverMemory"));
                }
            }

            if (!remote) {
                context = new JavaSparkContext(sparkConf);
                context.hadoopConfiguration().set(FileInputFormat.INPUT_DIR_RECURSIVE, Boolean.TRUE.toString());
            }

            if (repl) {
                new Local(config, getExeName(), getVersion(), getReplPrompt(), context).loop();
            } else if (remote) {
                new Client(config, getExeName(), getVersion(), getReplPrompt()).loop();
            } else {
                if (!serve && !config.hasOption("script")) {
                    throw new RuntimeException("No script to execute in the batch mode was specified");
                }

                if (config.hasOption("script")) {
                    new Runner(config, context).run();
                }

                if (serve) {
                    new Server(config, getVersion(), context).serve();

                    context = null;
                }
            }
        } catch (Exception ex) {
            if (ex instanceof ParseException) {
                config.printHelp(getExeName(), getVersion());
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
