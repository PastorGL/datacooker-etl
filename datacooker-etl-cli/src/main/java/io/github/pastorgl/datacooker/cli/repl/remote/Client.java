/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.cli.repl.remote;

import io.github.pastorgl.datacooker.cli.Configuration;
import io.github.pastorgl.datacooker.cli.Helper;
import io.github.pastorgl.datacooker.cli.repl.*;
import io.github.pastorgl.datacooker.data.StreamLineage;
import io.github.pastorgl.datacooker.metadata.*;
import io.github.pastorgl.datacooker.scripting.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Stream;

public class Client extends REPL {
    final Map<String, String> PACKAGE_CACHE = new LinkedHashMap<>();
    final Map<String, TransformMeta> TRANSFORM_CACHE = new LinkedHashMap<>();
    final Map<String, OperationMeta> OPERATION_CACHE = new LinkedHashMap<>();
    final Map<String, InputAdapterMeta> INPUT_CACHE = new LinkedHashMap<>();
    final Map<String, OutputAdapterMeta> OUTPUT_CACHE = new LinkedHashMap<>();
    final Map<String, EvaluatorInfo> OPERATOR_CACHE = new LinkedHashMap<>();
    final Map<String, EvaluatorInfo> FUNCTION_CACHE = new LinkedHashMap<>();

    public Client(Configuration config, String exeName, String version, String replPrompt) {
        super(config, exeName, version, replPrompt);

        String host = config.hasOption("host") ? config.getOptionValue("host") : "localhost";
        int port = config.hasOption("port") ? Utils.parseNumber(config.getOptionValue("port")).intValue() : 9595;

        final Requester rq = new Requester(host, port);

        String serverVersion = rq.get("version", String.class);

        if (!version.equals(serverVersion)) {
            Helper.log(new String[]{"Server " + host + ":" + port + " reports mismatched version " + serverVersion}, true);
        } else {
            Helper.log(new String[]{"Connecting to server " + host + ":" + port});
        }

        {
            rq.get("package/enum", List.class).forEach(p -> PACKAGE_CACHE.put((String) p, null));
            rq.get("transform/enum", List.class).forEach(t -> TRANSFORM_CACHE.put((String) t, null));
            rq.get("operation/enum", List.class).forEach(o -> OPERATION_CACHE.put((String) o, null));
            rq.get("input/enum", List.class).forEach(ia -> INPUT_CACHE.put((String) ia, null));
            rq.get("output/enum", List.class).forEach(oa -> OUTPUT_CACHE.put((String) oa, null));
            rq.get("operator/enum", List.class).forEach(op -> OPERATOR_CACHE.put((String) op, null));
            rq.get("function/enum", List.class).forEach(f -> FUNCTION_CACHE.put((String) f, null));
        }

        vp = new VariableProvider() {
            @Override
            public Set<String> getAll() {
                return new LinkedHashSet<String>(rq.get("variable/enum", List.class));
            }

            @Override
            public VariableInfo getVar(String name) {
                return rq.get("variable", VariableInfo.class, Collections.singletonMap("name", name));
            }
        };
        op = new OptionsProvider() {
            @Override
            public Set<String> getAll() {
                return new LinkedHashSet<String>(rq.get("options/enum", List.class));
            }

            @Override
            public OptionsInfo get(String name) {
                return rq.get("options", OptionsInfo.class, Collections.singletonMap("name", name));
            }
        };
        dp = new DataProvider() {
            @Override
            public Set<String> getAll() {
                return new LinkedHashSet<String>(rq.get("ds/enum", List.class));
            }

            @Override
            public boolean has(String dsName) {
                return get(dsName) != null;
            }

            @Override
            public StreamInfo get(String dsName) {
                return rq.get("ds", StreamInfo.class, Collections.singletonMap("name", dsName));
            }

            @Override
            public Stream<String> sample(String dsName, int limit) {
                return rq.get("ds/sample", List.class, Map.of("name", dsName, "limit", limit)).stream();
            }

            @Override
            public Stream<String> part(String dsName, int part, int limit) {
                return rq.get("ds/part", List.class, Map.of("name", dsName, "part", part, "limit", limit)).stream();
            }

            @Override
            public StreamInfo persist(String dsName) {
                return rq.post("ds/persist", dsName, StreamInfo.class);
            }

            @Override
            public void renounce(String dsName) {
                rq.get("ds/renounce", Void.class, Collections.singletonMap("name", dsName));
            }

            @Override
            public List<StreamLineage> lineage(String dsName) {
                return rq.get("ds/lineage", List.class, Collections.singletonMap("name", dsName));
            }
        };
        ep = new EntityProvider() {
            @Override
            public Set<String> getAllPackages() {
                return PACKAGE_CACHE.keySet();
            }

            @Override
            public Set<String> getAllTransforms() {
                return TRANSFORM_CACHE.keySet();
            }

            @Override
            public Set<String> getAllOperations() {
                return OPERATION_CACHE.keySet();
            }

            @Override
            public Set<String> getAllInputs() {
                return INPUT_CACHE.keySet();
            }

            @Override
            public Set<String> getAllOutputs() {
                return OUTPUT_CACHE.keySet();
            }

            @Override
            public Set<String> getAllOperators() {
                return OPERATOR_CACHE.keySet();
            }

            @Override
            public Set<String> getAllFunctions() {
                return FUNCTION_CACHE.keySet();
            }

            @Override
            public boolean hasPackage(String name) {
                return PACKAGE_CACHE.containsKey(name);
            }

            @Override
            public boolean hasTransform(String name) {
                return TRANSFORM_CACHE.containsKey(name);
            }

            @Override
            public boolean hasOperation(String name) {
                return OPERATION_CACHE.containsKey(name);
            }

            @Override
            public boolean hasInput(String name) {
                return INPUT_CACHE.containsKey(name);
            }

            @Override
            public boolean hasOutput(String name) {
                return OUTPUT_CACHE.containsKey(name);
            }

            @Override
            public boolean hasOperator(String symbol) {
                return OPERATOR_CACHE.containsKey(symbol);
            }

            @Override
            public boolean hasFunction(String symbol) {
                return FUNCTION_CACHE.containsKey(symbol);
            }

            @Override
            public String getPackage(String name) {
                String p = PACKAGE_CACHE.get(name);
                if (p == null) {
                    p = rq.get("package", String.class, Collections.singletonMap("name", name));
                }
                return p;
            }

            @Override
            public TransformMeta getTransform(String name) {
                TransformMeta t = TRANSFORM_CACHE.get(name);
                if (t == null) {
                    t = rq.get("transform", TransformMeta.class, Collections.singletonMap("name", name));
                }
                return t;
            }

            @Override
            public OperationMeta getOperation(String name) {
                OperationMeta o = OPERATION_CACHE.get(name);
                if (o == null) {
                    o = rq.get("operation", OperationMeta.class, Collections.singletonMap("name", name));
                }
                return o;
            }

            @Override
            public InputAdapterMeta getInput(String name) {
                InputAdapterMeta ia = INPUT_CACHE.get(name);
                if (ia == null) {
                    ia = rq.get("input", InputAdapterMeta.class, Collections.singletonMap("name", name));
                }
                return ia;
            }

            @Override
            public OutputAdapterMeta getOutput(String name) {
                OutputAdapterMeta oa = OUTPUT_CACHE.get(name);
                if (oa == null) {
                    oa = rq.get("output", OutputAdapterMeta.class, Collections.singletonMap("name", name));
                }
                return oa;
            }

            @Override
            public EvaluatorInfo getOperator(String symbol) {
                EvaluatorInfo ei = OPERATOR_CACHE.get(symbol);
                if (ei == null) {
                    ei = rq.get("operator", EvaluatorInfo.class, Collections.singletonMap("name", symbol));
                }
                return ei;
            }

            @Override
            public EvaluatorInfo getFunction(String symbol) {
                EvaluatorInfo oa = FUNCTION_CACHE.get(symbol);
                if (oa == null) {
                    oa = rq.get("function", EvaluatorInfo.class, Collections.singletonMap("name", symbol));
                }
                return oa;
            }
        };
        exp = new ExecutorProvider() {
            @Override
            public Object interpretExpr(String expr) {
                return rq.put("exec/expr", expr, String.class);
            }

            @Override
            public String readDirect(String path) {
                try {
                    Path source = Path.of(path);

                    return Files.readString(source);
                } catch (Exception e) {
                    throw new RuntimeException("Error while reading local file", e);
                }
            }

            @Override
            public String read(String pathExpr) {
                try {
                    Path source = Path.of(String.valueOf(interpretExpr(pathExpr)));

                    return Files.readString(source);
                } catch (Exception e) {
                    throw new RuntimeException("Error while reading local file", e);
                }
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
                rq.post("exec/script", script, String.class);
            }

            @Override
            public TDL4ErrorListener parse(String script) {
                return rq.post("exec/parse", script, TDL4ErrorListener.class);
            }

            @Override
            public List<String> getAllProcedures() {
                return rq.get("exec/procedure/enum", List.class);
            }

            @Override
            public Map<String, Param> getProcedure(String name) {
                Procedure proc = rq.get("exec/procedure", Procedure.class, Collections.singletonMap("name", name));
                return (proc != null) ? proc.params : null;
            }
        };
    }
}
