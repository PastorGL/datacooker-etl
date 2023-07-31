/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.cli;

import io.github.pastorgl.datacooker.cli.repl.REPL;
import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.data.DataContext;
import io.github.pastorgl.datacooker.scripting.TDL4ErrorListener;
import io.github.pastorgl.datacooker.scripting.TDL4Interpreter;
import io.github.pastorgl.datacooker.scripting.VariablesContext;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.StageInfo;
import org.apache.spark.storage.RDDInfo;
import scala.collection.JavaConverters;

import java.net.URL;
import java.util.*;
import java.util.jar.Manifest;
import java.util.stream.Collectors;

public class Main {
    private static final Logger LOG = Logger.getLogger(Main.class);

    protected String getExeName() {
        return "Data Cooker ETL";
    }

    protected String getVersion() {
        try {
            URL url = getClass().getClassLoader().getResource("META-INF/MANIFEST.MF");
            Manifest man = new Manifest(url.openStream());
            return man.getMainAttributes().getValue("Implementation-Version");
        } catch (Exception e) {
            return "unknown";
        }
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

            SparkConf sparkConf = new SparkConf()
                    .setAppName(getExeName())
                    .set("spark.serializer", org.apache.spark.serializer.KryoSerializer.class.getCanonicalName());

            boolean repl = config.hasOption("repl");
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

            context = new JavaSparkContext(sparkConf);
            context.hadoopConfiguration().set(FileInputFormat.INPUT_DIR_RECURSIVE, Boolean.TRUE.toString());

            if (repl) {
                REPL.run(config, context, getReplPrompt(), getExeName(), getVersion());
            } else {
                if (!config.hasOption("script")) {
                    LOG.error("No script to execute was specified");

                    System.exit(2);
                }

                String script = config.script(context, config.getOptionValue("script"));

                if (config.hasOption("dry")) {
                    TDL4ErrorListener errorListener = new TDL4ErrorListener();

                    new TDL4Interpreter(script, config.variables(context), new VariablesContext(), errorListener);

                    if (errorListener.errorCount > 0) {
                        List<String> errors = new ArrayList<>();
                        for (int i = 0; i < errorListener.errorCount; i++) {
                            errors.add("'" + errorListener.messages.get(i) + "' @ " + errorListener.lines.get(i) + ":" + errorListener.positions.get(i));
                        }

                        throw new InvalidConfigurationException("Invalid TDL4 script: " + errorListener.errorCount + " error(s).\n" +
                                String.join("\n", errors));
                    } else {
                        LOG.error("Input TDL4 script syntax check passed");
                    }
                } else {
                    final Map<String, Long> recordsRead = new HashMap<>();
                    final Map<String, Long> recordsWritten = new HashMap<>();

                    context.sc().addSparkListener(new SparkListener() {
                        @Override
                        public void onStageCompleted(SparkListenerStageCompleted stageCompleted) {
                            StageInfo stageInfo = stageCompleted.stageInfo();

                            long rR = stageInfo.taskMetrics().inputMetrics().recordsRead();
                            long rW = stageInfo.taskMetrics().outputMetrics().recordsWritten();
                            List<RDDInfo> infos = JavaConverters.seqAsJavaList(stageInfo.rddInfos());
                            List<String> rddNames = infos.stream()
                                    .map(RDDInfo::name)
                                    .filter(Objects::nonNull)
                                    .filter(n -> n.startsWith("datacooker:"))
                                    .collect(Collectors.toList());
                            if (rR > 0) {
                                rddNames.forEach(name -> recordsRead.compute(name, (n, r) -> (r == null) ? rR : rR + r));
                            }
                            if (rW > 0) {
                                rddNames.forEach(name -> recordsWritten.compute(name, (n, w) -> (w == null) ? rW : rW + w));
                            }
                        }
                    });

                    VariablesContext variables = config.variables(context);

                    TDL4ErrorListener errorListener = new TDL4ErrorListener();
                    TDL4Interpreter tdl4 = new TDL4Interpreter(script, variables, new VariablesContext(), errorListener);
                    if (errorListener.errorCount > 0) {
                        throw new InvalidConfigurationException("Invalid TDL4 script: " + errorListener.errorCount + " error(s). First error is '" + errorListener.messages.get(0)
                                + "' @ " + errorListener.lines.get(0) + ":" + errorListener.positions.get(0));
                    }

                    tdl4.interpret(new DataContext(context));

                    LOG.info("Raw physical record statistics");
                    recordsRead.forEach((key, value) -> LOG.info("Input '" + key + "': " + value + " record(s) read"));
                    recordsWritten.forEach((key, value) -> LOG.info("Output '" + key + "': " + value + " records(s) written"));
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
