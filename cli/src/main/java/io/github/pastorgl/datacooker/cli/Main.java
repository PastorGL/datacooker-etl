/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.cli;

import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.data.DataContext;
import io.github.pastorgl.datacooker.scripting.*;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.StageInfo;
import org.apache.spark.storage.RDDInfo;
import scala.jdk.javaapi.CollectionConverters;

import java.util.*;
import java.util.stream.Collectors;

public class Main {
    private static final Logger LOG = Logger.getLogger(Main.class);
    static final String CLI_NAME = "Data Cooker Command Line Interface";

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        Configuration config = new Configuration();

        JavaSparkContext context = null;
        try {
            config.setCommandLine(args, CLI_NAME);

            SparkConf sparkConf = new SparkConf()
                    .setAppName(CLI_NAME)
                    .set("spark.serializer", org.apache.spark.serializer.KryoSerializer.class.getCanonicalName());

            boolean local = config.hasOption("local");
            if (local) {
                String cores = "*";
                if (config.hasOption("localCores")) {
                    cores = config.getOptionValue("localCores");
                }

                sparkConf
                        .setMaster("local[" + cores + "]")
                        .set("spark.network.timeout", "10000");

                if (config.hasOption("driverMemory")) {
                    sparkConf.set("spark.driver.memory", config.getOptionValue("driverMemory"));
                }
                sparkConf.set("spark.ui.enabled", String.valueOf(config.hasOption("sparkUI")));
            }

            context = new JavaSparkContext(sparkConf);
            context.hadoopConfiguration().set(FileInputFormat.INPUT_DIR_RECURSIVE, Boolean.TRUE.toString());

            ScriptHolder script = config.build(context);

            if (config.hasOption("dry")) {
                CharStream cs = CharStreams.fromString(script.script);
                TDL4Lexicon lexer = new TDL4Lexicon(cs);
                TDL4 parser = new TDL4(new CommonTokenStream(lexer));

                TDL4ErrorListener errorListener = new TDL4ErrorListener();
                parser.addErrorListener(errorListener);

                parser.script();

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
                        List<RDDInfo> infos = CollectionConverters.asJava(stageInfo.rddInfos());
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

                TDL4Interpreter tdl4 = new TDL4Interpreter(script);
                tdl4.initialize(new DataContext(context));
                tdl4.interpret();

                LOG.info("Raw physical record statistics");
                recordsRead.forEach((key, value) -> LOG.info("Input '" + key + "': " + value + " record(s) read"));
                recordsWritten.forEach((key, value) -> LOG.info("Output '" + key + "': " + value + " records(s) written"));
            }
        } catch (Exception ex) {
            if (ex instanceof ParseException) {
                config.printHelp(CLI_NAME);
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
