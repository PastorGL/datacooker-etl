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
import io.github.pastorgl.datacooker.metadata.*;
import io.github.pastorgl.datacooker.scripting.Operations;
import io.github.pastorgl.datacooker.scripting.TDL4ErrorListener;
import io.github.pastorgl.datacooker.scripting.TDL4Interpreter;
import io.github.pastorgl.datacooker.scripting.VariablesContext;
import io.github.pastorgl.datacooker.storage.Adapters;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaSparkContext;
import org.jline.reader.EndOfFileException;
import org.jline.reader.History;
import org.jline.reader.LineReader;
import org.jline.reader.impl.history.DefaultHistory;
import org.jline.terminal.TerminalBuilder;
import scala.Tuple2;

import java.io.IOError;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

import static io.github.pastorgl.datacooker.cli.repl.Command.*;
import static io.github.pastorgl.datacooker.cli.repl.ReplCompleter.unescapeId;

public class REPL {
    private static String getWelcomeText(String exeName, String version) {
        String wel = exeName + " REPL interactive (ver. " + version + ")";

        return "\n\n" + StringUtils.repeat("=", wel.length()) + "\n" +
                wel + "\n" +
                "Type TDL4 statements to be executed in the REPL context in order of input, or a command.\n" +
                "Statement must always end with a semicolon. If not, it'll be continued on a next line.\n" +
                "If you want to type several statements at once on several lines, end each line with \\\n" +
                "Type \\QUIT; to end session and \\HELP; for list of all REPL commands and shortcuts\n";
    }

    public static void run(Configuration config, JavaSparkContext context, String replPrompt, String exeName, String version) throws Exception {
        Path historyPath = config.hasOption("history")
                ? Path.of(config.getOptionValue("history"))
                : Path.of(System.getProperty("user.home") + "/." + replPrompt + ".history");

        VariablesContext variablesContext = config.variables(context);
        variablesContext.put("CWD", Path.of("").toAbsolutePath().toString());
        VariablesContext options = new VariablesContext();
        options.put(Options.log_level.name(), "WARN");
        DataContext dataContext = new DataContext(context);
        dataContext.initialize(options);

        ReplCompleter completer = new ReplCompleter(variablesContext, dataContext);
        ReplParser parser = new ReplParser();
        AtomicBoolean ctrlC = new AtomicBoolean(false);
        ReplHighlighter highlighter = new ReplHighlighter();
        ReplLineReader reader = new ReplLineReader(ctrlC, TerminalBuilder.terminal(),
                exeName + " REPL", Map.of(
                LineReader.HISTORY_FILE, historyPath.toString(),
                LineReader.FEATURES_MAX_BUFFER_SIZE, 1024 * 1024
        ));
        reader.setParser(parser);
        reader.setCompleter(completer);
        reader.setOpt(LineReader.Option.CASE_INSENSITIVE);
        reader.setOpt(LineReader.Option.COMPLETE_IN_WORD);
        reader.setHighlighter(highlighter);

        reader.printAbove("Preparing REPL...");

        History history = new DefaultHistory();
        history.attach(reader);

        Util.populateEntities();

        reader.printAbove(getWelcomeText(exeName, version));

        boolean autoExec = config.hasOption("script");
        boolean dry = config.hasOption("dry");
        String line = autoExec ? "\\< '" + config.getOptionValue("script") + "';" : "";
        String cur;
        boolean contd = false, rec = false;
        StringBuilder record = new StringBuilder();
        Matcher matcher;
        String prompt, mainPr = replPrompt + "> ", contdPr = StringUtils.leftPad("| ", mainPr.length(), " ");
        while (true) {
            try {
                if (autoExec) {
                    reader.printAbove("Executing command line script '" + config.getOptionValue("script") + "'");
                    autoExec = false;
                } else {
                    if (!contd) {
                        line = "";
                        parser.reset();
                    }

                    prompt = contd ? contdPr : mainPr;

                    cur = reader.readLine(prompt, rec ? "[R]" : null, (Character) null, null).trim();
                    if (ctrlC.get()) {
                        ctrlC.set(false);
                        parser.reset();
                        line = "";
                        contd = false;
                        continue;
                    }
                    if (cur.isEmpty()) {
                        continue;
                    }

                    line += cur;
                    parser.update(cur);
                    if (cur.endsWith(";")) {
                        contd = false;
                    } else {
                        if (cur.endsWith("\\")) {
                            line = line.substring(0, line.lastIndexOf("\\"));
                        }
                        contd = true;
                        continue;
                    }
                }

                if (line.startsWith("\\")) {
                    line = line.substring(1, line.lastIndexOf(';')).trim();
                    matcher = QUIT.matcher(line);
                    if (matcher.matches()) {
                        break;
                    }

                    matcher = HELP.matcher(line);
                    if (matcher.matches()) {
                        String cmd = matcher.group("cmd");

                        Command c = Command.get(cmd);
                        if (c != null) {
                            reader.printAbove(c.descr());
                        } else {
                            reader.printAbove(Command.HELP_TEXT);
                        }

                        continue;
                    }

                    matcher = SHOW.matcher(line);
                    if (matcher.matches()) {
                        String ent = matcher.group("ent").toUpperCase();
                        show:
                        {
                            if ("DS".startsWith(ent)) {
                                reader.printAbove(String.join(", ", dataContext.getAll()) + "\n");
                                break show;
                            }
                            if ("VARIABLES".startsWith(ent)) {
                                reader.printAbove(String.join(", ", variablesContext.getAll()) + "\n");
                                break show;
                            }
                            if ("PACKAGES".startsWith(ent)) {
                                reader.printAbove(String.join(", ", RegisteredPackages.REGISTERED_PACKAGES.keySet()) + "\n");
                                break show;
                            }
                            if ("TRANSFORMS".startsWith(ent)) {
                                reader.printAbove(String.join(", ", Transforms.TRANSFORMS.keySet()) + "\n");
                                break show;
                            }
                            if ("OPERATIONS".startsWith(ent)) {
                                reader.printAbove(String.join(", ", Operations.OPERATIONS.keySet()) + "\n");
                                break show;
                            }
                            if ("INPUT".startsWith(ent)) {
                                reader.printAbove(String.join(", ", Adapters.INPUTS.keySet()) + "\n");
                                break show;
                            }
                            if ("OUTPUT".startsWith(ent)) {
                                reader.printAbove(String.join(", ", Adapters.OUTPUTS.keySet()) + "\n");
                                break show;
                            }
                            if ("OPTIONS".startsWith(ent)) {
                                reader.printAbove(Arrays.stream(Options.values()).map(Enum::name).collect(Collectors.joining(", ")) + "\n");
                                break show;
                            }

                            reader.printAbove(SHOW.descr());
                        }

                        continue;
                    }

                    matcher = DESCRIBE.matcher(line);
                    if (matcher.matches()) {
                        String ent = matcher.group("ent").toUpperCase();
                        String name = unescapeId(matcher.group("name"));
                        desc:
                        {
                            if ("DS".startsWith(ent)) {
                                if (dataContext.has(name)) {
                                    DataStream ds = dataContext.get(name);

                                    StringBuilder sb = new StringBuilder(ds.streamType + ", " + ds.rdd.getNumPartitions() + " partition(s)\n");
                                    for (Map.Entry<String, List<String>> cat : ds.accessor.attributes().entrySet()) {
                                        sb.append(StringUtils.capitalize(cat.getKey()) + " attributes:\n\t" + String.join(", ", cat.getValue()) + "\n");
                                    }
                                    sb.append(ds.getUsages() + " usage(s) with threshold of " + dataContext.usageThreshold() + ", " + ds.rdd.getStorageLevel() + "\n");

                                    reader.printAbove(sb.toString());
                                }
                                break desc;
                            }
                            if ("VARIABLES".startsWith(ent)) {
                                Set<String> all = variablesContext.getAll();
                                if (all.contains(name)) {
                                    Object val = variablesContext.getVar(name);

                                    if (val != null) {
                                        reader.printAbove(val.getClass().getSimpleName() + "\n");
                                        if (val.getClass().isArray()) {
                                            reader.printAbove(Arrays.toString((Object[]) val) + "\n");
                                        } else {
                                            reader.printAbove(val + "\n");
                                        }
                                    } else {
                                        reader.printAbove("NULL\n");
                                    }
                                }
                                break desc;
                            }
                            if ("PACKAGES".startsWith(ent)) {
                                if (RegisteredPackages.REGISTERED_PACKAGES.containsKey(name)) {
                                    reader.printAbove(RegisteredPackages.REGISTERED_PACKAGES.get(name) + "\n");
                                }
                                break desc;
                            }
                            if ("TRANSFORMS".startsWith(ent)) {
                                if (Transforms.TRANSFORMS.containsKey(name)) {
                                    TransformMeta meta = Transforms.TRANSFORMS.get(name).meta;

                                    StringBuilder sb = new StringBuilder();
                                    sb.append("Transforms " + meta.from + " -> " + meta.to + "\n");
                                    sb.append(meta.descr + "\n");
                                    sb.append("Keying: " + (meta.keyAfter() ? "after" : "before") + " transform\n");
                                    describeDefinitions(meta, sb);

                                    if (meta.transformed != null) {
                                        Map<String, String> gen = meta.transformed.streams.generated;
                                        if (!gen.isEmpty()) {
                                            sb.append("Generated attributes:\n");
                                            gen.forEach((key, value) -> sb.append("\t" + key + " " + value + "\n"));
                                        }
                                    }

                                    reader.printAbove(sb.toString());
                                }
                                break desc;
                            }
                            if ("OPERATIONS".startsWith(ent)) {
                                if (Operations.OPERATIONS.containsKey(name)) {
                                    OperationMeta meta = Operations.OPERATIONS.get(name).meta;

                                    StringBuilder sb = new StringBuilder();
                                    sb.append(meta.descr + "\n");

                                    sb.append("Inputs:\n");
                                    describeStreams(meta.input, sb);

                                    describeDefinitions(meta, sb);

                                    sb.append("Outputs:\n");
                                    describeStreams(meta.output, sb);

                                    reader.printAbove(sb.toString());
                                }
                                break desc;
                            }
                            if ("INPUT".startsWith(ent)) {
                                if (Adapters.INPUTS.containsKey(name)) {
                                    InputAdapterMeta meta = Adapters.INPUTS.get(name).meta;

                                    StringBuilder sb = new StringBuilder();
                                    sb.append("Produces: " + meta.type[0] + "\n");
                                    sb.append(meta.descr + "\n");
                                    sb.append("Path examples: " + String.join(", ", meta.paths) + "\n");
                                    describeDefinitions(meta, sb);

                                    reader.printAbove(sb.toString());
                                }
                                break desc;
                            }
                            if ("OUTPUT".startsWith(ent)) {
                                if (Adapters.OUTPUTS.containsKey(name)) {
                                    OutputAdapterMeta meta = Adapters.OUTPUTS.get(name).meta;

                                    StringBuilder sb = new StringBuilder();
                                    sb.append("Consumes: " + Arrays.stream(meta.type).map(Enum::name).collect(Collectors.joining(", ")) + "\n");
                                    sb.append(meta.descr + "\n");
                                    sb.append("Path examples: " + String.join(", ", meta.paths) + "\n");
                                    describeDefinitions(meta, sb);

                                    reader.printAbove(sb.toString());
                                }
                                break desc;
                            }
                            if ("OPTIONS".startsWith(ent)) {
                                if (Arrays.stream(Options.values()).map(Enum::name).anyMatch(e -> e.equals(name))) {
                                    StringBuilder sb = new StringBuilder();
                                    sb.append(Options.valueOf(name).descr() + "\n");
                                    sb.append("Default: " + Options.valueOf(name).def() + "\n");
                                    sb.append("Current: " + options.getString(name) + "\n");

                                    reader.printAbove(sb.toString());
                                }
                                break desc;
                            }

                            reader.printAbove(DESCRIBE.descr());
                        }

                        continue;
                    }

                    matcher = EVAL.matcher(line);
                    if (matcher.matches()) {
                        String expr = matcher.group("expr");

                        TDL4ErrorListener errorListener = new TDL4ErrorListener();
                        TDL4Interpreter tdl4 = new TDL4Interpreter(expr, variablesContext, options, errorListener);
                        try {
                            Object result = tdl4.interpretExpr();
                            reader.printAbove(result + "\n");
                        } catch (Exception e) {
                            reader.printAbove(e.getMessage() + "\n");
                        }

                        continue;
                    }

                    matcher = RECORD.matcher(line);
                    if (matcher.matches()) {
                        rec = true;

                        continue;
                    }

                    matcher = FLUSH.matcher(line);
                    if (matcher.matches()) {
                        if (rec) {
                            String expr = matcher.group("expr");
                            if (expr != null) {
                                TDL4ErrorListener errorListener = new TDL4ErrorListener();
                                TDL4Interpreter tdl4 = new TDL4Interpreter(expr, variablesContext, options, errorListener);
                                try {
                                    String path = String.valueOf(tdl4.interpretExpr());

                                    Path flush = Path.of(path);

                                    Files.writeString(flush, record);

                                    rec = false;
                                    record = new StringBuilder();
                                } catch (Exception e) {
                                    reader.printAbove("Error while flushing the recording to '" + expr + "': " + e.getMessage());
                                    continue;
                                }
                            } else {
                                rec = false;
                                record = new StringBuilder();
                            }
                        }

                        continue;
                    }

                    matcher = PRINT.matcher(line);
                    if (matcher.matches()) {
                        String ds = matcher.group("ds");
                        String num = matcher.group("num");

                        int limit = 5;
                        if (num != null) {
                            limit = Integer.parseInt(num);
                        }

                        if (dataContext.has(ds)) {
                            List<Tuple2<Object, Record<?>>> result = dataContext.get(ds).rdd.takeSample(false, limit);
                            for (Tuple2<Object, Record<?>> r : result) {
                                reader.printAbove(r._1 + " => " + r._2 + "\n");
                            }
                        } else {
                            reader.printAbove("There is no DS named '" + ds + "'\n");
                        }

                        continue;
                    }

                    // This must be last command case. If good, evaluation continues past command if line.startsWith('\\')
                    matcher = SCRIPT.matcher(line);
                    if (matcher.matches()) {
                        String expr = matcher.group("expr");

                        TDL4ErrorListener errorListener = new TDL4ErrorListener();
                        TDL4Interpreter tdl4 = new TDL4Interpreter(expr, variablesContext, options, errorListener);
                        try {
                            String path = String.valueOf(tdl4.interpretExpr());

                            line = config.script(context, path);
                        } catch (Exception e) {
                            reader.printAbove(e.getMessage() + "\n");
                            continue;
                        }
                    } else {
                        reader.printAbove("Unrecognized command '\\" + line + ";'\nType \\HELP; to get list of available commands\n");
                        continue;
                    }
                }

                TDL4ErrorListener errorListener = new TDL4ErrorListener();
                TDL4Interpreter tdl4 = new TDL4Interpreter(line, variablesContext, options, errorListener);
                if (errorListener.errorCount > 0) {
                    List<String> errors = new ArrayList<>();
                    for (int i = 0; i < errorListener.errorCount; i++) {
                        errors.add("'" + errorListener.messages.get(i) + "' @ " + errorListener.lines.get(i) + ":" + errorListener.positions.get(i));
                    }

                    reader.printAbove(errorListener.errorCount + " error(s).\n" +
                            String.join("\n", errors));
                } else {
                    if (!dry) {
                        tdl4.interpret(dataContext);
                    }
                }
                dry = false;

                if (rec) {
                    record.append(line).append("\n");
                }
            } catch (IOError | EndOfFileException e) {
                break;
            } catch (Exception e) {
                reader.printAbove(e.getMessage());
            }
        }

        history.append(historyPath, true);
    }

    private static void describeStreams(DataStreamsMeta meta, StringBuilder sb) {
        Map<String, DataStreamMeta> streams = (meta instanceof PositionalStreamsMeta)
                ? Collections.singletonMap("", ((PositionalStreamsMeta) meta).streams)
                : ((NamedStreamsMeta) meta).streams;

        for (Map.Entry<String, DataStreamMeta> e : streams.entrySet()) {
            String name = e.getKey();
            DataStreamMeta stream = e.getValue();

            if (!name.isEmpty()) {
                sb.append("Named " + name + ":\n");
            } else {
                int max = ((PositionalStreamsMeta) meta).positional;
                sb.append("Positional, " + ((max > 0) ? "requires " + max : "unlimited number of") + " DS:\n");
            }
            sb.append("Types: " + Arrays.stream(stream.type).map(Enum::name).collect(Collectors.joining(", ")) + "\n");
            sb.append((stream.optional ? "Optional" : "Mandatory") + ((stream.origin != null) ? ", " + stream.origin + ((stream.ancestors != null) ? " from " + String.join(", ", stream.ancestors) : "") : "") + "\n");

            Map<String, String> gen = stream.generated;
            if (gen != null) {
                sb.append("Generated attributes:\n");
                gen.forEach((key, value) -> sb.append("\t" + key + " " + value + "\n"));
            }
        }
    }

    private static void describeDefinitions(ConfigurableMeta meta, StringBuilder sb) {
        if (meta.definitions != null) {
            sb.append("Parameters:\n");
            for (Map.Entry<String, DefinitionMeta> def : meta.definitions.entrySet()) {
                DefinitionMeta val = def.getValue();
                if (val.optional) {
                    sb.append("Optional " + val.hrType + " " + def.getKey() + " = " + val.defaults + " (" + val.defDescr + ")\n\t" + val.descr + "\n");
                } else if (val.dynamic) {
                    sb.append("Dynamic " + val.hrType + " prefix " + def.getKey() + "\n\t" + val.descr + "\n");
                } else {
                    sb.append("Mandatory " + val.hrType + " " + def.getKey() + "\n\t" + val.descr + "\n");
                }
                if (val.values != null) {
                    val.values.entrySet().forEach(e -> sb.append("\t\t" + e.getKey() + " " + e.getValue() + "\n"));
                }
            }
        }
    }
}
