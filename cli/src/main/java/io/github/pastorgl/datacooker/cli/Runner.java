/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.cli;

import io.github.pastorgl.datacooker.Options;
import io.github.pastorgl.datacooker.data.DataContext;
import io.github.pastorgl.datacooker.scripting.OptionsContext;
import io.github.pastorgl.datacooker.scripting.TDL4ErrorListener;
import io.github.pastorgl.datacooker.scripting.TDL4Interpreter;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.StageInfo;
import org.apache.spark.storage.RDDInfo;
import scala.collection.JavaConverters;

import java.util.*;
import java.util.stream.Collectors;

public class Runner {
    private final Configuration config;
    private final JavaSparkContext context;

    public Runner(Configuration config, JavaSparkContext context) {
        this.config = config;
        this.context = context;
    }

    public void run() throws Exception {
        String scriptName = config.getOptionValue("script");
        Helper.log(new String[]{"Loading command line script " + scriptName});

        String script = Helper.loadScript(scriptName, context);

        TDL4ErrorListener errorListener = new TDL4ErrorListener();
        TDL4Interpreter tdl4 = new TDL4Interpreter(script, Helper.loadVariables(config, context), new OptionsContext(Map.of(Options.batch_verbose.name(), Boolean.TRUE.toString())), errorListener);
        tdl4.parseScript();
        if (errorListener.errorCount > 0) {
            Helper.log(new String[]{
                    "Invalid TDL4 script: " + errorListener.errorCount + " error(s)",
                    "First error is '" + errorListener.messages.get(0) + "' @ " + errorListener.lines.get(0) + ":" + errorListener.positions.get(0)
            }, true);

            System.exit(2);
        } else {
            Helper.log(new String[]{"Command line script syntax check passed"});
        }

        if (!config.hasOption("dry")) {
            Helper.log(new String[]{"Executing command line script " + scriptName});

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

            tdl4.interpret(new DataContext(context));

            final List<String> stats = new ArrayList<>();
            stats.add("Raw physical record statistics");
            recordsRead.forEach((key, value) -> stats.add("Input '" + key + "': " + value + " record(s) read"));
            recordsWritten.forEach((key, value) -> stats.add("Output '" + key + "': " + value + " records(s) written"));
            Helper.log(stats.toArray(new String[0]));
        }
    }
}
