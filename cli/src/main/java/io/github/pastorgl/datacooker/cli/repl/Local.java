/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.cli.repl;

import io.github.pastorgl.datacooker.Options;
import io.github.pastorgl.datacooker.RegisteredPackages;
import io.github.pastorgl.datacooker.cli.Configuration;
import io.github.pastorgl.datacooker.data.DataContext;
import io.github.pastorgl.datacooker.data.DataStream;
import io.github.pastorgl.datacooker.data.Record;
import io.github.pastorgl.datacooker.data.Transforms;
import io.github.pastorgl.datacooker.metadata.InputAdapterMeta;
import io.github.pastorgl.datacooker.metadata.OperationMeta;
import io.github.pastorgl.datacooker.metadata.OutputAdapterMeta;
import io.github.pastorgl.datacooker.metadata.TransformMeta;
import io.github.pastorgl.datacooker.scripting.*;
import io.github.pastorgl.datacooker.storage.Adapters;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static io.github.pastorgl.datacooker.cli.Main.LOG;

public class Local extends REPL {
    public Local(Configuration config, JavaSparkContext context, String replPrompt, String exeName, String version) throws Exception {
        this.replPrompt = replPrompt;
        this.exeName = exeName;
        this.version = version;

        LOG.warn("Preparing Local REPL...");

        OptionsContext options = new OptionsContext();
        options.put(Options.log_level.name(), "WARN");

        DataContext dataContext = new DataContext(context);
        dataContext.initialize(options);

        VariablesContext vc = config.variables(context);
        vc.put("CWD", Path.of("").toAbsolutePath().toString());

        Util.populateEntities();

        vp = new VariableProvider() {
            @Override
            public Set<String> getAll() {
                return vc.getAll();
            }

            @Override
            public Object getVar(String name) {
                return vc.getVar(name);
            }
        };
        op = new OptionsProvider() {
            @Override
            public Set<String> getAll() {
                return Arrays.stream(Options.values()).map(Enum::name).collect(Collectors.toSet());
            }

            @Override
            public Object get(String name) {
                return options.getOption(name);
            }
        };
        dp = new DataProvider() {
            @Override
            public Set<String> getAll() {
                return dataContext.getAll();
            }

            @Override
            public boolean has(String dsName) {
                return dataContext.has(dsName);
            }

            @Override
            public DSData get(String dsName) {
                DataStream dataStream = dataContext.get(dsName);
                return new DSData(dataStream.accessor.attributes(), dataStream.rdd.getStorageLevel().description(),
                        dataStream.streamType.name(), dataStream.rdd.getNumPartitions(), dataStream.getUsages());
            }

            @Override
            public List<Tuple2<Object, Record<?>>> sample(String dsName, int limit) {
                return dataContext.get(dsName).rdd.takeSample(false, limit);
            }
        };
        ep = new EntityProvider() {
            @Override
            public Set<String> getAllPackages() {
                return RegisteredPackages.REGISTERED_PACKAGES.keySet();
            }

            @Override
            public Set<String> getAllTransforms() {
                return Transforms.TRANSFORMS.keySet();
            }

            @Override
            public Set<String> getAllOperations() {
                return Operations.OPERATIONS.keySet();
            }

            @Override
            public Set<String> getAllInputs() {
                return Adapters.INPUTS.keySet();
            }

            @Override
            public Set<String> getAllOutputs() {
                return Adapters.OUTPUTS.keySet();
            }

            @Override
            public boolean hasPackage(String name) {
                return RegisteredPackages.REGISTERED_PACKAGES.containsKey(name);
            }

            @Override
            public boolean hasTransform(String name) {
                return Transforms.TRANSFORMS.containsKey(name);
            }

            @Override
            public boolean hasOperation(String name) {
                return Operations.OPERATIONS.containsKey(name);
            }

            @Override
            public boolean hasInput(String name) {
                return Adapters.INPUTS.containsKey(name);
            }

            @Override
            public boolean hasOutput(String name) {
                return Adapters.OUTPUTS.containsKey(name);
            }

            @Override
            public String getPackage(String name) {
                return RegisteredPackages.REGISTERED_PACKAGES.get(name);
            }

            @Override
            public TransformMeta getTransform(String name) {
                return Transforms.TRANSFORMS.get(name).meta;
            }

            @Override
            public OperationMeta getOperation(String name) {
                return Operations.OPERATIONS.get(name).meta;
            }

            @Override
            public InputAdapterMeta getInput(String name) {
                return Adapters.INPUTS.get(name).meta;
            }

            @Override
            public OutputAdapterMeta getOutput(String name) {
                return Adapters.OUTPUTS.get(name).meta;
            }
        };
        exp = new ExecutorProvider() {
            @Override
            public Object interpretExpr(String expr) {
                TDL4ErrorListener errorListener = new TDL4ErrorListener();
                TDL4Interpreter tdl4 = new TDL4Interpreter(expr, vc, options, errorListener);

                return tdl4.interpretExpr();
            }

            @Override
            public String read(String pathExpr) {
                String path = String.valueOf(interpretExpr(pathExpr));

                return config.script(context, path);
            }

            @Override
            public void write(String pathExpr, String recording) throws Exception {
                String path = String.valueOf(interpretExpr(pathExpr));

                Path flush = Path.of(path);

                Files.writeString(flush, recording);
            }

            @Override
            public void interpret(String script) {
                new TDL4Interpreter(script, vc, options, new TDL4ErrorListener()).interpret(dataContext);
            }

            @Override
            public TDL4ErrorListener parse(String script) {
                TDL4ErrorListener errorListener = new TDL4ErrorListener();
                new TDL4Interpreter(script, vc, options, errorListener).parseScript();
                return errorListener;
            }
        };
    }
}
