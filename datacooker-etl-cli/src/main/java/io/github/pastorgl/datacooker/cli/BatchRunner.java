/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.cli;

import io.github.pastorgl.datacooker.Options;
import io.github.pastorgl.datacooker.data.DataContext;
import io.github.pastorgl.datacooker.scripting.*;
import org.apache.spark.api.java.JavaSparkContext;

public class BatchRunner {
    private final Configuration config;
    private final JavaSparkContext context;
    private final DataContext dataContext;
    private final Library library;
    private final OptionsContext optionsContext;
    private final VariablesContext variablesContext;

    public BatchRunner(Configuration config, JavaSparkContext context, DataContext dataContext, Library library, OptionsContext optionsContext, VariablesContext variablesContext) {
        this.config = config;
        this.context = context;
        this.dataContext = dataContext;
        this.library = library;
        this.optionsContext = optionsContext;
        this.variablesContext = variablesContext;
    }

    public void run() {
        String scriptName = config.getOptionValue("script");
        Helper.log(new String[]{"Loading command line script(s) " + scriptName});

        String script = Helper.loadScript(scriptName, context);

        optionsContext.put(Options.batch_verbose.name(), true);
        if (config.hasOption("local")) {
            optionsContext.put(Options.log_level.name(), "WARN");
        }

        TDLErrorListener errorListener = new TDLErrorListener();
        TDLInterpreter tdl = new TDLInterpreter(library, script, variablesContext, optionsContext, errorListener);
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

            tdl.interpret(dataContext);
        }
    }
}
