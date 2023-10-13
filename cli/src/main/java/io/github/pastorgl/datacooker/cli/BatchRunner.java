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

public class BatchRunner {
    private final Configuration config;
    private final JavaSparkContext context;

    public BatchRunner(Configuration config, JavaSparkContext context) {
        this.config = config;
        this.context = context;
    }

    public void run() throws Exception {
        String scriptName = config.getOptionValue("script");
        Helper.log(new String[]{"Loading command line script " + scriptName});

        String script = Helper.loadScript(scriptName, context);

        TDL4ErrorListener errorListener = new TDL4ErrorListener();
        OptionsContext optionsContext = new OptionsContext(Map.of(Options.batch_verbose.name(), Boolean.TRUE.toString()));
        if (config.hasOption("local")) {
            optionsContext.put(Options.log_level.name(), "WARN");
        }
        TDL4Interpreter tdl4 = new TDL4Interpreter(script, Helper.loadVariables(config, context), optionsContext, errorListener);
        tdl4.parseScript();
        if (errorListener.errorCount > 0) {
            Helper.log(new String[]{
                    "Command line script syntax check found " + errorListener.errorCount + " error(s)",
                    "First error at line " + errorListener.lines.get(0) + " position " + errorListener.positions.get(0) + ":",
                    errorListener.messages.get(0)
            }, true);

            System.exit(2);
        } else {
            Helper.log(new String[]{"Command line script syntax check passed"});
        }

        if (!config.hasOption("dry")) {
            Helper.log(new String[]{"Executing command line script " + scriptName});

            tdl4.interpret(new DataContext(context));
        }
    }
}
