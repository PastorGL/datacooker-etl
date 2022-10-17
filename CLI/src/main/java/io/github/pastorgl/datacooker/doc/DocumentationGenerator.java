/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.doc;

import io.github.pastorgl.datacooker.RegisteredPackages;
import io.github.pastorgl.datacooker.scripting.OperationInfo;
import io.github.pastorgl.datacooker.scripting.Operations;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;

import java.io.FileWriter;
import java.io.Writer;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;

public class DocumentationGenerator {
/*    public static TaskDefinitionLanguage.Task createExampleTask(OperationInfo opInfo, String prefix) throws Exception {
        List<String> input = new ArrayList<>();
        List<String> output = new ArrayList<>();

        TaskDefinitionLanguage.Task task = TaskDefinitionLanguage.createTask();
        TaskDefinitionLanguage.Operation opDef = TaskDefinitionLanguage.createOperation(task);

        OperationMeta meta = opInfo.meta;

        opDef.verb = meta.verb;
        opDef.name = meta.verb;

        Map<String, List<String>> namedColumns = new HashMap<>();
        namedColumns.put(null, new ArrayList<>());

        if (meta.definitions != null) {
            TaskDefinitionLanguage.Definitions defs = TaskDefinitionLanguage.createDefinitions(task);

            for (Map.Entry<String, DefinitionMeta> defMeta : meta.definitions.entrySet()) {
                DefinitionMeta def = defMeta.getValue();
                String name = defMeta.getKey();

                if (!def.dynamic) {
                    String value;

                    if (def.optional) {
                        value = "<* " + def.defaults + ": " + def.defDescr + " *>";
                    } else {
                        Class type = Class.forName(def.type);
                        if (type.isEnum()) {
                            value = Arrays.stream(type.getEnumConstants())
                                    .map(e -> ((Enum) e).name())
                                    .collect(Collectors.joining(", ", "<* One of: ", " *>"));
                        } else {
                            value = "<* " + type.getSimpleName() + ": " + def.defDescr + " *>";
                        }
                    }

                    if (name.endsWith(Constants.COLUMN_SUFFIX)) {
                        String[] col = name.split("\\.", 3);

                        if (col.length == 3) {
                            namedColumns.computeIfAbsent(col[0], x -> new ArrayList<>());
                            namedColumns.get(col[0]).add(col[1]);

                            value = meta.verb + "_" + col[0] + "." + col[1];
                        } else if (col.length == 2) {
                            namedColumns.get(null).add(col[0]);

                            value = meta.verb + "_0." + col[0];
                        }
                    }

                    defs.put(name, value);
                } else {
                    String value;
                    Class type = Class.forName(def.type);
                    if (type.isEnum()) {
                        value = Arrays.stream(type.getEnumConstants())
                                .map(e -> ((Enum) e).name())
                                .collect(Collectors.joining(", ", "<* One of: ", " *>"));
                    } else {
                        value = "<* " + type.getSimpleName() + ": " + def.defDescr + " *>";
                    }

                    defs.put(name + "*", value);
                }
            }

            opDef.definitions = defs;
        }

        AtomicInteger dsNum = new AtomicInteger();

        if (meta.input instanceof PositionalStreamsMeta) {
            PositionalStreamsMeta pInput = (PositionalStreamsMeta) meta.input;
            StreamType st = pInput.streams.type[0];

            List<String> posInputs = new ArrayList<>();
            int cnt = pInput.positional;
            for (int i = 0; i < cnt; i++) {
                String name = meta.verb + "_" + i;

                List<String> columns = null;
                if (pInput.streams.columnar) {
                    columns = new ArrayList<>();

                    List<String> named = namedColumns.get(null);
                    if ((named != null) && !named.isEmpty()) {
                        columns.addAll(named);
                    } else {
                        columns.add("<* A list of attributes *>");
                    }
                }

                createSourceOps(name, dsNum, st, columns, task, input);
                posInputs.add(name);
            }

            opDef.input.positional = String.join(",", posInputs);
        } else if (meta.input instanceof NamedStreamsMeta) {
            opDef.input.named = TaskDefinitionLanguage.createDefinitions(task);
            for (Map.Entry<String, DataStreamMeta> nsDef : ((NamedStreamsMeta) meta.input).streams.entrySet()) {
                String name = nsDef.getKey();
                DataStreamMeta nsDesc = nsDef.getValue();

                StreamType st = nsDesc.type[0];

                List<String> columns = null;
                if (nsDesc.columnar) {
                    columns = new ArrayList<>();

                    List<String> named = namedColumns.get(name);
                    if ((named != null) && !named.isEmpty()) {
                        columns.addAll(named);
                    } else {
                        columns.add("<* A list of attributes *>");
                    }
                }

                createSourceOps(meta.verb + "_" + name, dsNum, st, columns, task, input);

                opDef.input.named.put(name, meta.verb + "_" + name);
            }
        }

        task.items.add(opDef);

        if (meta.output instanceof PositionalStreamsMeta) {
            PositionalStreamsMeta pOutput = (PositionalStreamsMeta) meta.output;
            StreamType st = pOutput.streams.type[0];

            List<String> columns = null;
            if (pOutput.streams.columnar) {
                columns = new ArrayList<>();

                if (pOutput.streams.generated.size() > 0) {
                    columns.addAll(pOutput.streams.generated.keySet());
                } else {
                    columns.add("<* A list of attributes from input(s) goes here *>");
                }
            }

            createOutput(meta.verb, dsNum, st, columns, task, output);

            opDef.output.positional = meta.verb;
        } else if (meta.output instanceof NamedStreamsMeta) {
            opDef.output.named = TaskDefinitionLanguage.createDefinitions(task);
            for (Map.Entry<String, DataStreamMeta> nsDef : ((NamedStreamsMeta) meta.output).streams.entrySet()) {
                String name = nsDef.getKey();
                DataStreamMeta nsDesc = nsDef.getValue();
                StreamType st = nsDesc.type[0];

                List<String> columns = null;
                if (nsDesc.columnar) {
                    columns = new ArrayList<>();

                    if (nsDesc.generated.size() > 0) {
                        columns.addAll(nsDesc.generated.keySet());
                    } else {
                        columns.add("<* A list of attributes from input(s) goes here *>");
                    }
                }

                createOutput(name, dsNum, st, columns, task, output);

                opDef.output.named.put(name, name);
            }
        }

        task.setForeignLayer("output.path", "proto://full/path/to/output/directory");

        task.prefix = prefix;
        task.input = input;
        task.output = output;

        return task;
    }

    private static void createSourceOps(String name, AtomicInteger dsNum, StreamType st, List<String> columns, TaskDefinitionLanguage.Task task, List<String> input) {
        switch (st) {
            case Plain:
            case CSV: {
                TaskDefinitionLanguage.DataStream inp = new TaskDefinitionLanguage.DataStream();
                inp.input = new TaskDefinitionLanguage.StreamDesc();
                if (columns != null) {
                    inp.input.columns = String.join(Constants.COMMA, columns);
                    inp.input.delimiter = ",";
                }
                inp.input.partCount = "100500";

                task.setForeignLayer("input.path." + name, "proto://full/path/to/source_" + dsNum + "/*.csv");

                task.streams.put(name, inp);
                input.add(name);

                break;
            }
            case Point: {
                TaskDefinitionLanguage.Operation src = TaskDefinitionLanguage.createOperation(task);
                src.verb = "pointCsvSource";
                src.name = "source_" + dsNum.incrementAndGet();
                src.input.positional = "source_" + dsNum;
                src.output.positional = name;
                src.definitions = TaskDefinitionLanguage.createDefinitions(task);
                src.definitions.put("radius_column", "source_" + dsNum + ".radius");
                src.definitions.put("lat_column", "source_" + dsNum + ".lat");
                src.definitions.put("lon_column", "source_" + dsNum + ".lon");

                task.items.add(src);

                TaskDefinitionLanguage.DataStream inp = new TaskDefinitionLanguage.DataStream();
                inp.input = new TaskDefinitionLanguage.StreamDesc();
                inp.input.columns = "lat,lon,radius";
                inp.input.partCount = "100500";

                task.setForeignLayer("input.path." + name, "proto://full/path/to/source_" + dsNum + "/*.csv");

                task.streams.put("source_" + dsNum, inp);
                input.add("source_" + dsNum);

                TaskDefinitionLanguage.DataStream output = new TaskDefinitionLanguage.DataStream();
                if (columns != null) {
                    output.input = new TaskDefinitionLanguage.StreamDesc();
                    output.input.columns = String.join(Constants.COMMA, columns);
                    output.input.delimiter = ",";
                }

                task.streams.put(name, output);

                break;
            }
            case Track: {
                TaskDefinitionLanguage.Operation src = TaskDefinitionLanguage.createOperation(task);
                src.verb = "trackGpxSource";
                src.name = "source_" + dsNum.incrementAndGet();
                src.input.positional = "source_" + dsNum;
                src.output.positional = name;

                task.items.add(src);

                TaskDefinitionLanguage.DataStream inp = new TaskDefinitionLanguage.DataStream();
                inp.input = new TaskDefinitionLanguage.StreamDesc();
                inp.input.partCount = "100500";

                task.setForeignLayer("input.path." + name, "proto://full/path/to/source_" + dsNum + "/*.gpx");

                task.streams.put("source_" + dsNum, inp);
                input.add("source_" + dsNum);

                TaskDefinitionLanguage.DataStream output = new TaskDefinitionLanguage.DataStream();
                if (columns != null) {
                    output.input = new TaskDefinitionLanguage.StreamDesc();
                    output.input.columns = String.join(Constants.COMMA, columns);
                    output.input.delimiter = ",";
                }

                task.streams.put(name, output);

                break;
            }
            case Polygon: {
                TaskDefinitionLanguage.Operation src = TaskDefinitionLanguage.createOperation(task);
                src.verb = "polygonJsonSource";
                src.name = "source_" + dsNum.incrementAndGet();
                src.input.positional = "source_" + dsNum;
                src.output.positional = name;

                task.items.add(src);

                TaskDefinitionLanguage.DataStream inp = new TaskDefinitionLanguage.DataStream();
                inp.input = new TaskDefinitionLanguage.StreamDesc();
                inp.input.partCount = "100500";

                task.setForeignLayer("input.path." + name, "proto://full/path/to/source_" + dsNum + "/*.json");

                task.streams.put("source_" + dsNum, inp);
                input.add("source_" + dsNum);

                TaskDefinitionLanguage.DataStream output = new TaskDefinitionLanguage.DataStream();
                if (columns != null) {
                    output.input = new TaskDefinitionLanguage.StreamDesc();
                    output.input.columns = String.join(Constants.COMMA, columns);
                    output.input.delimiter = ",";
                }

                task.streams.put(name, output);

                break;
            }
            case KeyValue: {
                TaskDefinitionLanguage.DataStream inter = new TaskDefinitionLanguage.DataStream();
                inter.input = new TaskDefinitionLanguage.StreamDesc();
                inter.input.columns = "key,_";
                inter.input.delimiter = ",";
                inter.input.partCount = "100500";

                task.setForeignLayer("input.path." + name, "proto://full/path/to/source_" + dsNum + "/*.csv");

                task.streams.put(name + "_0", inter);
                input.add(name + "_0");

                TaskDefinitionLanguage.Operation toPair = TaskDefinitionLanguage.createOperation(task);
                toPair.verb = "mapToPair";
                toPair.name = "source_" + dsNum.incrementAndGet();
                toPair.input.positional = name + "_0";
                toPair.output.positional = name;
                toPair.definitions = TaskDefinitionLanguage.createDefinitions(task);
//                toPair.definitions.put(MapToPairOperation.OP_KEY_COLUMNS, name + "_0.key");

                task.items.add(toPair);

                TaskDefinitionLanguage.DataStream output = new TaskDefinitionLanguage.DataStream();
                if (columns != null) {
                    output.input = new TaskDefinitionLanguage.StreamDesc();
                    output.input.columns = String.join(Constants.COMMA, columns);
                    output.input.delimiter = ",";
                }

                task.streams.put(name, output);

                break;
            }
        }
    }

    private static void createOutput(String name, AtomicInteger dsNum, StreamType st, List<String> columns, TaskDefinitionLanguage.Task task, List<String> outputs) {
        switch (st) {
            case Point: {
                TaskDefinitionLanguage.Operation out = TaskDefinitionLanguage.createOperation(task);
                out.verb = "pointCsvOutput";
                out.name = "output_" + dsNum.incrementAndGet();
                out.input.positional = name;
                out.output.positional = name + "_output";

                task.items.add(out);

                TaskDefinitionLanguage.DataStream output = new TaskDefinitionLanguage.DataStream();
                if (columns != null) {
                    output.output = new TaskDefinitionLanguage.StreamDesc();
                    output.output.columns = String.join(Constants.COMMA, columns);
                    output.output.delimiter = ",";
                }

                task.streams.put(name + "_output", output);
                outputs.add(name + "_output");
                break;
            }
            case Track: {
                TaskDefinitionLanguage.Operation out = TaskDefinitionLanguage.createOperation(task);
                out.verb = "trackGpxOutput";
                out.name = "output_" + dsNum.incrementAndGet();
                out.input.positional = name;
                out.output.positional = name + "_output";

                task.items.add(out);

                TaskDefinitionLanguage.DataStream output = new TaskDefinitionLanguage.DataStream();
                output.output = new TaskDefinitionLanguage.StreamDesc();
                output.output.columns = name + ".lat," + name + ".lon," + name + ".radius";
                output.output.delimiter = ",";

                task.streams.put(name + "_output", output);
                outputs.add(name + "_output");
                break;
            }
            case Polygon: {
                TaskDefinitionLanguage.Operation out = TaskDefinitionLanguage.createOperation(task);
                out.verb = "polygonJsonOutput";
                out.name = "output_" + dsNum.incrementAndGet();
                out.input.positional = name;
                out.output.positional = name + "_output";

                task.items.add(out);

                TaskDefinitionLanguage.DataStream output = new TaskDefinitionLanguage.DataStream();
                if (columns != null) {
                    output.output = new TaskDefinitionLanguage.StreamDesc();
                    output.output.columns = String.join(Constants.COMMA, columns);
                    output.output.delimiter = ",";
                }

                task.streams.put(name + "_output", output);
                outputs.add(name + "_output");
                break;
            }
            default: {
                TaskDefinitionLanguage.DataStream output = new TaskDefinitionLanguage.DataStream();
                if (columns != null) {
                    output.output = new TaskDefinitionLanguage.StreamDesc();
                    output.output.columns = String.join(Constants.COMMA, columns);
                    output.output.delimiter = ",";
                }

                task.streams.put(name, output);
                outputs.add(name);
                break;
            }
        }
    }

    public static void packageDoc(String pkgName, Writer writer) {
        String descr = RegisteredPackages.REGISTERED_PACKAGES.get(pkgName);

        VelocityContext ic = new VelocityContext();
        ic.put("name", pkgName);
        ic.put("descr", descr);
        ic.put("ops", Operations.packageOperations(pkgName));
        ic.put("inputs", Adapters.packageInputs(pkgName));
        ic.put("outputs", Adapters.packageOutputs(pkgName));

        Template index = Velocity.getTemplate("package.vm", UTF_8.name());
        index.merge(ic, writer);
    }

    public static void indexDoc(Map<String, String> packages, FileWriter writer) {
        VelocityContext ic = new VelocityContext();
        ic.put("pkgs", packages);

        Template index = Velocity.getTemplate("index.vm", UTF_8.name());
        index.merge(ic, writer);
    }*/
}
