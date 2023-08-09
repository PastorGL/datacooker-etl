/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.cli.repl;

import io.github.pastorgl.datacooker.Options;
import io.github.pastorgl.datacooker.cli.Configuration;
import io.github.pastorgl.datacooker.metadata.*;
import io.github.pastorgl.datacooker.scripting.TDL4ErrorListener;
import org.apache.commons.lang3.StringUtils;
import org.jline.reader.EndOfFileException;
import org.jline.reader.History;
import org.jline.reader.LineReader;
import org.jline.reader.impl.history.DefaultHistory;
import org.jline.terminal.TerminalBuilder;

import java.io.IOError;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

import static io.github.pastorgl.datacooker.cli.repl.Command.*;
import static io.github.pastorgl.datacooker.cli.repl.ReplCompleter.unescapeId;

public abstract class REPL {
    protected Configuration config;

    protected String exeName, version, replPrompt;

    protected VariableProvider vp;
    protected OptionsProvider op;
    protected DataProvider dp;
    protected EntityProvider ep;
    protected ExecutorProvider exp;

    public REPL(Configuration config, String exeName, String version, String replPrompt) {
        this.config = config;
        this.exeName = exeName;
        this.version = version;
        this.replPrompt = replPrompt;
    }

    private String getWelcomeText() {
        String wel = exeName + " REPL interactive (ver. " + version + ")";

        return "\n\n" + StringUtils.repeat("=", wel.length()) + "\n" +
                wel + "\n" +
                "Type TDL4 statements to be executed in the REPL context in order of input, or a command.\n" +
                "Statement must always end with a semicolon. If not, it'll be continued on a next line.\n" +
                "If you want to type several statements at once on several lines, end each line with \\\n" +
                "Type \\QUIT; to end session and \\HELP; for list of all REPL commands and shortcuts\n";
    }

    public void loop() throws Exception {
        Path historyPath = config.hasOption("history")
                ? Path.of(config.getOptionValue("history"))
                : Path.of(System.getProperty("user.home") + "/." + replPrompt + ".history");

        ReplCompleter completer = new ReplCompleter(vp, dp);
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

        History history = new DefaultHistory();
        history.attach(reader);

        reader.printAbove(getWelcomeText());

        boolean autoExec = config.hasOption("script");
        boolean dry = config.hasOption("dry");
        String line = autoExec ? "\\< '" + config.getOptionValue("script") + "';" : "";
        String cur;
        boolean contd = false, rec = false;
        StringBuilder recorder = new StringBuilder();
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
                                reader.printAbove(String.join(", ", dp.getAll()) + "\n");
                                break show;
                            }
                            if ("VARIABLES".startsWith(ent)) {
                                reader.printAbove(String.join(", ", vp.getAll()) + "\n");
                                break show;
                            }
                            if ("PACKAGES".startsWith(ent)) {
                                reader.printAbove(String.join(", ", ep.getAllPackages()) + "\n");
                                break show;
                            }
                            if ("TRANSFORMS".startsWith(ent)) {
                                reader.printAbove(String.join(", ", ep.getAllTransforms()) + "\n");
                                break show;
                            }
                            if ("OPERATIONS".startsWith(ent)) {
                                reader.printAbove(String.join(", ", ep.getAllOperations()) + "\n");
                                break show;
                            }
                            if ("INPUT".startsWith(ent)) {
                                reader.printAbove(String.join(", ", ep.getAllInputs()) + "\n");
                                break show;
                            }
                            if ("OUTPUT".startsWith(ent)) {
                                reader.printAbove(String.join(", ", ep.getAllOutputs()) + "\n");
                                break show;
                            }
                            if ("OPTIONS".startsWith(ent)) {
                                reader.printAbove(String.join(", ", op.getAll()) + "\n");
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
                                if (dp.has(name)) {
                                    StreamInfo ds = dp.get(name);

                                    StringBuilder sb = new StringBuilder(ds.streamType + ", " + ds.numPartitions + " partition(s)\n");
                                    for (Map.Entry<String, List<String>> cat : ds.attrs.entrySet()) {
                                        sb.append(StringUtils.capitalize(cat.getKey()) + " attributes:\n\t" + String.join(", ", cat.getValue()) + "\n");
                                    }
                                    sb.append(ds.usages + " usage(s) with threshold of " + op.get(Options.usage_threshold.name()) + ", " + ds.sl + "\n");

                                    reader.printAbove(sb.toString());
                                }
                                break desc;
                            }
                            if ("VARIABLES".startsWith(ent)) {
                                Set<String> all = vp.getAll();
                                if (all.contains(name)) {
                                    Object val = vp.getVar(name);

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
                                if (ep.hasPackage(name)) {
                                    reader.printAbove(ep.getPackage(name) + "\n");
                                }
                                break desc;
                            }
                            if ("TRANSFORMS".startsWith(ent)) {
                                if (ep.hasTransform(name)) {
                                    TransformMeta meta = ep.getTransform(name);

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
                                if (ep.hasOperation(name)) {
                                    OperationMeta meta = ep.getOperation(name);

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
                                if (ep.hasInput(name)) {
                                    InputAdapterMeta meta = ep.getInput(name);

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
                                if (ep.hasOutput(name)) {
                                    OutputAdapterMeta meta = ep.getOutput(name);

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
                                    sb.append("Current: " + op.get(name) + "\n");

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

                        try {
                            Object result = exp.interpretExpr(expr);

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
                                try {
                                    exp.write(expr, recorder.toString());

                                    rec = false;
                                    recorder = new StringBuilder();
                                } catch (Exception e) {
                                    reader.printAbove("Error while flushing the recording to '" + expr + "': " + e.getMessage());
                                    continue;
                                }
                            } else {
                                rec = false;
                                recorder = new StringBuilder();
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

                        if (dp.has(ds)) {
                            dp.sample(ds, limit)
                                    .forEach(r -> reader.printAbove(r + "\n"));
                        } else {
                            reader.printAbove("There is no DS named '" + ds + "'\n");
                        }

                        continue;
                    }

                    // This must be last command case. If good, evaluation continues past command if line.startsWith('\\')
                    matcher = SCRIPT.matcher(line);
                    if (matcher.matches()) {
                        String expr = matcher.group("expr");

                        try {
                            line = exp.read(expr);
                        } catch (Exception e) {
                            reader.printAbove(e.getMessage() + "\n");
                            continue;
                        }
                    } else {
                        reader.printAbove("Unrecognized command '\\" + line + ";'\nType \\HELP; to get list of available commands\n");
                        continue;
                    }
                }

                TDL4ErrorListener errorListener = exp.parse(line);
                if (errorListener.errorCount > 0) {
                    List<String> errors = new ArrayList<>();
                    for (int i = 0; i < errorListener.errorCount; i++) {
                        errors.add("'" + errorListener.messages.get(i) + "' @ " + errorListener.lines.get(i) + ":" + errorListener.positions.get(i));
                    }

                    reader.printAbove(errorListener.errorCount + " error(s).\n" +
                            String.join("\n", errors));
                } else {
                    if (!dry) {
                        exp.interpret(line);
                    }
                }
                dry = false;

                if (rec) {
                    recorder.append(line).append("\n");
                }
            } catch (IOError | EndOfFileException e) {
                break;
            } catch (Exception e) {
                String message = e.getMessage();
                reader.printAbove(((message == null) ? "Caught exception of type " + e.getClass() : message) + "\n");
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
