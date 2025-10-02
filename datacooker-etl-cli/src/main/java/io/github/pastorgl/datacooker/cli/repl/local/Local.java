/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.cli.repl.local;

import io.github.pastorgl.datacooker.Options;
import io.github.pastorgl.datacooker.PackageInfo;
import io.github.pastorgl.datacooker.RegisteredPackages;
import io.github.pastorgl.datacooker.cli.Configuration;
import io.github.pastorgl.datacooker.cli.Helper;
import io.github.pastorgl.datacooker.cli.repl.*;
import io.github.pastorgl.datacooker.data.DataContext;
import io.github.pastorgl.datacooker.data.DataHelper;
import io.github.pastorgl.datacooker.data.StreamLineage;
import io.github.pastorgl.datacooker.metadata.FunctionInfo;
import io.github.pastorgl.datacooker.metadata.OperatorInfo;
import io.github.pastorgl.datacooker.metadata.PluggableMeta;
import io.github.pastorgl.datacooker.metadata.Pluggables;
import io.github.pastorgl.datacooker.scripting.*;
import org.apache.spark.api.java.JavaSparkContext;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;

public class Local extends REPL {
    public Local(Configuration config, String exeName, String version, String replPrompt, JavaSparkContext context) throws Exception {
        super(config, exeName, version, replPrompt);

        Helper.log(new String[]{"Preparing Local REPL..."});

        OptionsContext.put(Options.log_level.name(), "WARN");

        Helper.populateEntities();

        vp = new VariableProvider() {
            @Override
            public Set<String> getAll() {
                return VariablesContext.global().getAll();
            }

            @Override
            public VariableInfo getVar(String name) {
                return VariablesContext.global().varInfo(name);
            }
        };
        op = new OptionsProvider() {
            @Override
            public Set<String> getAll() {
                return Options.getAll();
            }

            @Override
            public OptionsInfo get(String name) {
                if (Arrays.stream(Options.values()).map(Enum::name).anyMatch(e -> e.equals(name))) {
                    return new OptionsInfo(Options.valueOf(name), OptionsContext.getOption(name));
                }
                return null;
            }
        };
        dp = new DataProvider() {
            @Override
            public Set<String> getAll() {
                return DataContext.getWildcard();
            }

            @Override
            public boolean has(String dsName) {
                return DataContext.has(dsName);
            }

            @Override
            public StreamInfo get(String dsName) {
                return DataContext.streamInfo(dsName);
            }

            @Override
            public Stream<String> sample(String dsName, int limit) {
                return DataContext.get(dsName).rdd().takeSample(false, limit).stream()
                        .map(t -> t._1 + " => " + t._2);
            }

            @Override
            public Stream<String> part(String dsName, final int part, final int limit) {
                return DataHelper.takeFromPart(DataContext.get(dsName).rdd(), part, limit);
            }

            @Override
            public StreamInfo persist(String dsName) {
                return DataContext.persist(dsName);
            }

            @Override
            public void renounce(String dsName) {
                DataContext.renounce(dsName);
            }

            @Override
            public List<StreamLineage> lineage(String dsName) {
                return DataContext.get(dsName).lineage;
            }
        };
        ep = new EntityProvider() {
            @Override
            public Set<String> getAllPackages() {
                return RegisteredPackages.REGISTERED_PACKAGES.keySet();
            }

            @Override
            public Set<String> getAllTransforms() {
                TreeSet<String> all = new TreeSet<>(Pluggables.TRANSFORMS.keySet());
                all.addAll(Library.TRANSFORMS.keySet());
                return all;
            }

            @Override
            public Set<String> getAllOperations() {
                return Pluggables.OPERATIONS.keySet();
            }

            @Override
            public Set<String> getAllInputs() {
                return Pluggables.INPUTS.keySet();
            }

            @Override
            public Set<String> getAllOutputs() {
                return Pluggables.OUTPUTS.keySet();
            }

            @Override
            public Set<String> getAllOperators() {
                return Operators.OPERATORS.keySet();
            }

            @Override
            public Set<String> getAllFunctions() {
                TreeSet<String> all = new TreeSet<>(Functions.FUNCTIONS.keySet());
                all.addAll(Library.FUNCTIONS.keySet());
                return all;
            }

            @Override
            public boolean hasPackage(String name) {
                return RegisteredPackages.REGISTERED_PACKAGES.containsKey(name);
            }

            @Override
            public boolean hasTransform(String name) {
                return Pluggables.TRANSFORMS.containsKey(name) || Library.TRANSFORMS.containsKey(name);
            }

            @Override
            public boolean hasOperation(String name) {
                return Pluggables.OPERATIONS.containsKey(name);
            }

            @Override
            public boolean hasInput(String name) {
                return Pluggables.INPUTS.containsKey(name);
            }

            @Override
            public boolean hasOutput(String name) {
                return Pluggables.OUTPUTS.containsKey(name);
            }

            @Override
            public boolean hasOperator(String symbol) {
                return Operators.OPERATORS.containsKey(symbol);
            }

            @Override
            public boolean hasFunction(String symbol) {
                return Functions.FUNCTIONS.containsKey(symbol) || Library.FUNCTIONS.containsKey(symbol);
            }

            @Override
            public PackageInfo getPackage(String name) {
                return RegisteredPackages.REGISTERED_PACKAGES.get(name);
            }

            @Override
            public PluggableMeta getTransform(String name) {
                if (Pluggables.TRANSFORMS.containsKey(name)) {
                    return Pluggables.TRANSFORMS.get(name).meta;
                }

                if (Library.TRANSFORMS.containsKey(name)) {
                    return Library.TRANSFORMS.get(name).meta;
                }

                return null;
            }

            @Override
            public PluggableMeta getOperation(String name) {
                return Pluggables.OPERATIONS.get(name).meta;
            }

            @Override
            public PluggableMeta getInput(String name) {
                return Pluggables.INPUTS.get(name).meta;
            }

            @Override
            public PluggableMeta getOutput(String name) {
                return Pluggables.OUTPUTS.get(name).meta;
            }

            @Override
            public OperatorInfo getOperator(String symbol) {
                return Operators.OPERATORS.get(symbol);
            }

            @Override
            public FunctionInfo getFunction(String symbol) {
                if (Functions.FUNCTIONS.containsKey(symbol)) {
                    return Functions.FUNCTIONS.get(symbol);
                }

                if (Library.FUNCTIONS.containsKey(symbol)) {
                    return Library.FUNCTIONS.get(symbol);
                }

                return null;
            }
        };

        exp = new ExecutorProvider() {
            @Override
            public Object interpretExpr(String expr) {
                TDLErrorListener errorListener = new TDLErrorListener();
                TDLInterpreter tdl = new TDLInterpreter(expr, errorListener);

                return tdl.interpretExpr(VariablesContext.global());
            }

            @Override
            public String readDirect(String path) {
                return Helper.loadScript(path, context);
            }

            @Override
            public String read(String pathExpr) {
                String path = String.valueOf(interpretExpr(pathExpr));

                return Helper.loadScript(path, context);
            }

            @Override
            public void write(String pathExpr, String recording) {
                try {
                    String path = String.valueOf(interpretExpr(pathExpr));

                    Path flush = Path.of(path);

                    Files.writeString(flush, recording);
                } catch (Exception e) {
                    throw new RuntimeException("Error while writing local file", e);
                }
            }

            @Override
            public void interpret(String script) {
                new TDLInterpreter(script, new TDLErrorListener()).interpret();
            }

            @Override
            public TDLErrorListener parse(String script) {
                TDLErrorListener errorListener = new TDLErrorListener();
                new TDLInterpreter(script, errorListener).parseScript();
                return errorListener;
            }

            @Override
            public List<String> getAllProcedures() {
                return Library.PROCEDURES.keySet().stream().toList();
            }

            @Override
            public Procedure getProcedure(String name) {
                return Library.PROCEDURES.getOrDefault(name, null);
            }
        };
    }
}
