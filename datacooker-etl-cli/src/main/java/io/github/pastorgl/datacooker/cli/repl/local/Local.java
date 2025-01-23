/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.cli.repl.local;

import io.github.pastorgl.datacooker.Options;
import io.github.pastorgl.datacooker.RegisteredPackages;
import io.github.pastorgl.datacooker.cli.Configuration;
import io.github.pastorgl.datacooker.cli.Helper;
import io.github.pastorgl.datacooker.cli.repl.*;
import io.github.pastorgl.datacooker.data.DataHelper;
import io.github.pastorgl.datacooker.data.DataContext;
import io.github.pastorgl.datacooker.data.StreamLineage;
import io.github.pastorgl.datacooker.data.Transforms;
import io.github.pastorgl.datacooker.metadata.*;
import io.github.pastorgl.datacooker.scripting.*;
import io.github.pastorgl.datacooker.storage.Adapters;
import org.apache.spark.api.java.JavaSparkContext;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static io.github.pastorgl.datacooker.Constants.CWD_VAR;

public class Local extends REPL {
    public Local(Configuration config, String exeName, String version, String replPrompt, JavaSparkContext context, DataContext dataContext, Library library, OptionsContext optionsContext, VariablesContext vc) throws Exception {
        super(config, exeName, version, replPrompt);

        Helper.log(new String[]{"Preparing Local REPL..."});

        optionsContext.put(Options.log_level.name(), "WARN");

        vc.put(CWD_VAR, Path.of("").toAbsolutePath().toString());

        Helper.populateEntities();

        vp = new VariableProvider() {
            @Override
            public Set<String> getAll() {
                return vc.getAll();
            }

            @Override
            public VariableInfo getVar(String name) {
                return vc.varInfo(name);
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
                    return new OptionsInfo(Options.valueOf(name), optionsContext.getOption(name));
                }
                return null;
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
            public StreamInfo get(String dsName) {
                return dataContext.streamInfo(dsName);
            }

            @Override
            public Stream<String> sample(String dsName, int limit) {
                return dataContext.rdd(dsName).takeSample(false, limit).stream()
                        .map(t -> t._1 + " => " + t._2);
            }

            @Override
            public Stream<String> part(String dsName, final int part, final int limit) {
                return DataHelper.takeFromPart(dataContext.rdd(dsName), part, limit);
            }

            @Override
            public StreamInfo persist(String dsName) {
                return dataContext.persist(dsName);
            }

            @Override
            public void renounce(String dsName) {
                dataContext.renounce(dsName);
            }

            @Override
            public List<StreamLineage> lineage(String dsName) {
                return dataContext.get(dsName).lineage;
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
            public Set<String> getAllOperators() {
                return Operators.OPERATORS.keySet();
            }

            @Override
            public Set<String> getAllFunctions() {
                return Functions.FUNCTIONS.keySet();
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
            public boolean hasOperator(String symbol) {
                return Operators.OPERATORS.containsKey(symbol);
            }

            @Override
            public boolean hasFunction(String symbol) {
                return Functions.FUNCTIONS.containsKey(symbol);
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

            @Override
            public EvaluatorInfo getOperator(String symbol) {
                return EvaluatorInfo.bySymbol(symbol);
            }

            @Override
            public EvaluatorInfo getFunction(String symbol) {
                return EvaluatorInfo.bySymbol(symbol);
            }
        };

        exp = new ExecutorProvider() {
            @Override
            public Object interpretExpr(String expr) {
                TDL4ErrorListener errorListener = new TDL4ErrorListener();
                TDL4Interpreter tdl4 = new TDL4Interpreter(library, expr, vc, optionsContext, errorListener);

                return tdl4.interpretExpr();
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
                new TDL4Interpreter(library, script, vc, optionsContext, new TDL4ErrorListener()).interpret(dataContext);
            }

            @Override
            public TDL4ErrorListener parse(String script) {
                TDL4ErrorListener errorListener = new TDL4ErrorListener();
                new TDL4Interpreter(library, script, vc, optionsContext, errorListener).parseScript();
                return errorListener;
            }

            @Override
            public List<String> getAllProcedures() {
                return library.procedures.keySet().stream().toList();
            }

            @Override
            public Map<String, Param> getProcedure(String name) {
                return library.procedures.containsKey(name) ? library.procedures.get(name).params : null;
            }
        };
    }
}
