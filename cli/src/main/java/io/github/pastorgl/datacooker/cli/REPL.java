/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.cli;

import io.github.pastorgl.datacooker.RegisteredPackages;
import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
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
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static io.github.pastorgl.datacooker.cli.TDL4Completer.unescapeId;

public class REPL {
    static final Pattern QUIT = Pattern.compile("(exit|quit|q|!).*", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    static final Pattern HELP = Pattern.compile("(help|h|\\?)(?:\\s+(?<cmd>.+))?", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    static final Pattern PRINT = Pattern.compile("(print|p|:)\\s+(?<ds>.+?)(?:\\s+(?<num>\\d+))?", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    static final Pattern EVAL = Pattern.compile("(eval|e|=)\\s+(?<expr>.+)", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    static final Pattern SHOW = Pattern.compile("(show|list|l|\\|)\\s+(?<ent>.+)", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    static final Pattern DESCRIBE = Pattern.compile("(describe|desc|d|!)\\s+(?<ent>.+?)\\s+(?<name>.+)", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    static final Pattern SCRIPT = Pattern.compile("(script|source|s|<)\\s+(?<expr>.+)", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    static final Pattern RECORD = Pattern.compile("(record|start|r|\\[)", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    static final Pattern FLUSH = Pattern.compile("(flush|stop|f|])(:?\\s+(?<expr>.+))?", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    private static final Map<String, String> HELP_TEXT = new HashMap<>() {{
        put("", "Available REPL commands:\n" +
                "    \\QUIT; to end session\n" +
                "    \\HELP [\\COMMAND]; for \\COMMAND's help screen\n" +
                "    \\EVAL <TDL4_expression>; to evaluate a TDL4 expression\n" +
                "    \\PRINT <ds_name> [num_records]; to print a sample of num_records from data set ds_name\n" +
                "    \\SHOW <entity>; where entity is one of DS|Variable|Package|Operation|Transform|Input|Output\n" +
                "                    to list entities available in the current REPL session\n" +
                "    \\DESCRIBE <entity> <name>; to describe an entity referenced by its name\n" +
                "    \\SCRIPT <source_expression>; to load and execute a TDL4 script from the designated source\n" +
                "    \\RECORD; to start recording operators\n" +
                "    \\FLUSH [<file_expression>]; to stop recording (and optionally save it to designated file)\n" +
                "Available shortcuts:\n" +
                "    [Ctrl C] then [Enter] to abort currently unfinished line(s)\n" +
                "    [Ctrl D] to abort input\n" +
                "    [Ctrl R] to reverse search in history\n" +
                "    [Up] and [Down] to scroll through history\n" +
                "    [!!] to repeat last line, [!n] to repeat n-th line from the last\n");
        put("\\QUIT", "\\QUIT;\n" +
                "    End current session and quit the REPL\n" +
                "    Aliases: \\EXIT, \\Q, \\!\n");
        put("\\HELP", "\\HELP [\\COMMAND];\n" +
                "    Display help screen on a selected \\COMMAND or list all available commands\n" +
                "    Aliases: \\H, \\?\n");
        put("\\EVAL", "\\EVAL <TDL4_expression>;\n" +
                "    Evaluate a TDL4 expression in the REPL context. Can reference any set $VARIABLES\n" +
                "    Aliases: \\E, \\=\n");
        put("\\PRINT", "\\PRINT <ds_name> [num_records];\n" +
                "    Sample random records from the referenced data set, and print them as key => value pairs.\n" +
                "    By default, 5 records are selected. Use TDL4 ANALYZE to retrieve number of records in a set\n" +
                "    Aliases: \\P, \\:\n");
        put("\\SHOW", "\\SHOW <entity>;\n" +
                "    List entities available in the current REPL session (only two first letters are significant):\n" +
                "        DS current Data Sets in the REPL context\n" +
                "        VAriables in the top-level REPL context\n" +
                "        PAckages in the classpath\n" +
                "        OPerations\n" +
                "        TRansforms\n" +
                "        INput|OUtput Storage Adapters\n" +
                "    Aliases: \\LIST, \\L, \\|\n");
        put("\\DESCRIBE", "\\DESCRIBE <entity> <name>;\n" +
                "    Provides a description of an entity referenced by its name. For list of entities, see \\SHOW\n" +
                "    Aliases: \\DESC, \\D, \\!\n");
        put("\\SCRIPT", "\\SCRIPT <source_expression>;\n" +
                "    Load a script from the source which name is referenced by expression (evaluated to String),\n" +
                "    parse, and execute it in REPL context operator by operator\n" +
                "    Aliases: \\SOURCE, \\S, \\<\n");
        put("\\RECORD", "\\RECORD;\n" +
                "    Start recording TDL4 operators in the order of input. If operator execution ends with error,\n" +
                "    it won't be recorded. If you type multiple operators at once, they are recorded together\n" +
                "    Operators loaded by \\SCRIPT, if successful, will be recorded as a single chunk as well\n" +
                "    Aliases: \\START, \\R, \\[\n");
        put("\\FLUSH", "\\FLUSH [<file_expression>];\n" +
                "    Stop recording. Save record to file which name is referenced by expression (evaluated to String)\n" +
                "    Aliases: \\STOP, \\F, \\]\n");
    }};

    private static String getWelcomeText(String exeName) {
        return "\n\n================================\n" +
                exeName + " REPL interactive\n" +
                "Type TDL4 statements to be executed in the REPL context in order of input, or a command.\n" +
                "Statement must always end with a semicolon. If not, it'll be continued on a next line.\n" +
                "If you want to type several statements at once on several lines, end each line with \\\n" +
                "Type \\QUIT; to end session and \\HELP; for list of all REPL commands and shortcuts\n";
    }

    public static void run(Configuration config, JavaSparkContext context, String replPrompt, String exeName) throws Exception {
        Path historyPath = config.hasOption("history")
                ? Path.of(config.getOptionValue("history"))
                : Path.of(System.getProperty("user.home") + "/." + replPrompt + ".history");

        VariablesContext variablesContext = config.variables(context);
        variablesContext.put("CWD", Path.of("").toAbsolutePath().toString());
        VariablesContext options = new VariablesContext();
        options.put(DataContext.OPT_LOG_LEVEL, "WARN");
        DataContext dataContext = new DataContext(context);
        dataContext.initialize(options);

        TDL4Completer completer = new TDL4Completer(variablesContext, dataContext);
        TDL4Parser parser = new TDL4Parser();
        AtomicBoolean ctrlC = new AtomicBoolean(false);
        TDL4Highlighter highlighter = new TDL4Highlighter();
        TDL4LineReader reader = new TDL4LineReader(ctrlC, TerminalBuilder.terminal(),
                exeName + " REPL", Map.of(LineReader.HISTORY_FILE, historyPath.toString()));
        reader.setParser(parser);
        reader.setCompleter(completer);
        reader.setOpt(LineReader.Option.CASE_INSENSITIVE);
        reader.setOpt(LineReader.Option.COMPLETE_IN_WORD);
        reader.setHighlighter(highlighter);

        reader.printAbove("Preparing REPL...");

        History history = new DefaultHistory();
        history.attach(reader);

        {
            RegisteredPackages.REGISTERED_PACKAGES.size();
            Adapters.INPUTS.size();
            Transforms.TRANSFORMS.size();
            Operations.OPERATIONS.size();
            Adapters.OUTPUTS.size();
        }

        reader.printAbove(getWelcomeText(replPrompt));

        boolean autoExec = config.hasOption("script");
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
                        if (cmd != null) {
                            cmd = cmd.toUpperCase();
                            if (!HELP_TEXT.containsKey(cmd)) {
                                cmd = "";
                            }
                        } else {
                            cmd = "";
                        }
                        reader.printAbove(HELP_TEXT.get(cmd));

                        continue;
                    }

                    matcher = SHOW.matcher(line);
                    if (matcher.matches()) {
                        String ent = matcher.group("ent").toUpperCase().substring(0, 2);
                        switch (ent) {
                            case "DS": {
                                reader.printAbove(String.join(", ", dataContext.getAll()) + "\n");
                                break;
                            }
                            case "VA": {
                                reader.printAbove(String.join(", ", variablesContext.getAll()) + "\n");
                                break;
                            }
                            case "IN": {
                                reader.printAbove(String.join(", ", Adapters.INPUTS.keySet()) + "\n");
                                break;
                            }
                            case "OU": {
                                reader.printAbove(String.join(", ", Adapters.OUTPUTS.keySet()) + "\n");
                                break;
                            }
                            case "TR": {
                                reader.printAbove(String.join(", ", Transforms.TRANSFORMS.keySet()) + "\n");
                                break;
                            }
                            case "OP": {
                                reader.printAbove(String.join(", ", Operations.OPERATIONS.keySet()) + "\n");
                                break;
                            }
                            case "PA": {
                                reader.printAbove(String.join(", ", RegisteredPackages.REGISTERED_PACKAGES.keySet()) + "\n");
                                break;
                            }
                            default: {
                                reader.printAbove(HELP_TEXT.get("\\SHOW"));
                            }
                        }

                        continue;
                    }

                    matcher = DESCRIBE.matcher(line);
                    if (matcher.matches()) {
                        String ent = matcher.group("ent").toUpperCase().substring(0, 2);
                        String name = unescapeId(matcher.group("name"));
                        switch (ent) {
                            case "DS": {
                                if (dataContext.has(name)) {
                                    DataStream ds = dataContext.get(name);

                                    StringBuilder sb = new StringBuilder(ds.streamType + ", " + ds.rdd.getNumPartitions() + " partition(s)\n");
                                    for (Map.Entry<String, List<String>> cat : ds.accessor.attributes().entrySet()) {
                                        sb.append(StringUtils.capitalize(cat.getKey()) + " attributes:\n\t" + String.join(", ", cat.getValue()) + "\n");
                                    }
                                    sb.append(ds.getUsages() + " usage(s) with threshold of " + dataContext.usageThreshold() + ", " + ds.rdd.getStorageLevel() + "\n");

                                    reader.printAbove(sb.toString());
                                }
                                break;
                            }
                            case "VA": {
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
                                break;
                            }
                            case "IN": {
                                if (Adapters.INPUTS.containsKey(name)) {
                                    InputAdapterMeta meta = Adapters.INPUTS.get(name).meta;

                                    StringBuilder sb = new StringBuilder();
                                    sb.append("Produces: " + meta.type[0] + "\n");
                                    sb.append(meta.descr + "\n");
                                    sb.append(meta.path + "\n");
                                    describeDefinitions(meta, sb);

                                    reader.printAbove(sb.toString());
                                }
                                break;
                            }
                            case "OU": {
                                if (Adapters.OUTPUTS.containsKey(name)) {
                                    OutputAdapterMeta meta = Adapters.OUTPUTS.get(name).meta;

                                    StringBuilder sb = new StringBuilder();
                                    sb.append("Consumes: " + Arrays.stream(meta.type).map(Enum::name).collect(Collectors.joining(", ")) + "\n");
                                    sb.append(meta.descr + "\n");
                                    sb.append(meta.path + "\n");
                                    describeDefinitions(meta, sb);

                                    reader.printAbove(sb.toString());
                                }
                                break;
                            }
                            case "TR": {
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
                                break;
                            }
                            case "OP": {
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
                                break;
                            }
                            case "PA": {
                                if (RegisteredPackages.REGISTERED_PACKAGES.containsKey(name)) {
                                    reader.printAbove(RegisteredPackages.REGISTERED_PACKAGES.get(name) + "\n");
                                }
                                break;
                            }
                            default: {
                                reader.printAbove(HELP_TEXT.get("\\DESCRIBE"));
                            }
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
                    tdl4.interpret(dataContext);
                }

                if (rec) {
                    record.append(line).append("\n");
                }
            } catch (InvalidConfigurationException | IllegalArgumentException e) {
                reader.printAbove(e.getMessage());
            } catch (IOError | EndOfFileException e) {
                break;
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
                    val.values.entrySet().forEach(e -> sb.append("\t" + e.getKey() + " " + e.getValue() + "\n"));
                }
            }
        }
    }
}
