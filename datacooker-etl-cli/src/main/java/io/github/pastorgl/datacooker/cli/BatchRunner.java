/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.cli;

import io.github.pastorgl.datacooker.Options;
import io.github.pastorgl.datacooker.scripting.OptionsContext;
import io.github.pastorgl.datacooker.scripting.TDLErrorListener;
import io.github.pastorgl.datacooker.scripting.TDLInterpreter;
import org.apache.spark.api.java.JavaSparkContext;

public class BatchRunner {
    private final Configuration config;
    private final JavaSparkContext context;

    public BatchRunner(Configuration config, JavaSparkContext context) {
        this.config = config;
        this.context = context;
    }

    public void run() {
        String scriptName = config.getOptionValue("script");
        Helper.log(new String[]{"Loading command line script(s) " + scriptName});

        String script = Helper.loadScript(scriptName, context);

        OptionsContext.put(Options.batch_verbose.name(), true);
        if (config.hasOption("local")) {
            OptionsContext.put(Options.log_level.name(), "WARN");
        }

        TDLErrorListener errorListener = new TDLErrorListener();
        TDLInterpreter tdl = new TDLInterpreter(script, errorListener);
        tdl.parseScript();
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
            Helper.log(new String[]{"Executing command line script(s) " + scriptName});

            tdl.interpret();
        }
    }
}
