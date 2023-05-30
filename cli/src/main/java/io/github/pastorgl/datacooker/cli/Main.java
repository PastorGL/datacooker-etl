/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.cli;

import io.github.pastorgl.datacooker.RegisteredPackages;
import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.data.DataContext;
import io.github.pastorgl.datacooker.data.Record;
import io.github.pastorgl.datacooker.data.Transforms;
import io.github.pastorgl.datacooker.scripting.Operations;
import io.github.pastorgl.datacooker.scripting.TDL4ErrorListener;
import io.github.pastorgl.datacooker.scripting.TDL4Interpreter;
import io.github.pastorgl.datacooker.scripting.VariablesContext;
import io.github.pastorgl.datacooker.storage.Adapters;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.StageInfo;
import org.apache.spark.storage.RDDInfo;
import org.jline.reader.*;
import org.jline.reader.impl.DefaultParser;
import org.jline.reader.impl.history.DefaultHistory;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import scala.Tuple2;
import scala.collection.JavaConverters;

import java.io.IOError;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Main {
    private static final Logger LOG = Logger.getLogger(Main.class);
    static final Pattern QUIT = Pattern.compile("(exit|quit|q|!).*", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    static final Pattern HELP = Pattern.compile("(help|h|\\?)(?:\\s+(?<cmd>.+))?", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    static final Pattern PRINT = Pattern.compile("(print|p|:)\\s+(?<ds>.+?)(?:\\s+(?<num>\\d+))?", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    static final Pattern EVAL = Pattern.compile("(eval|e|=)\\s+(?<expr>.+)", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    static final Pattern LIST = Pattern.compile("(show|list|l|\\|)\\s+(?<ent>.+)", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
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
                "    \\SCRIPT <source_expression>; to load and execute a TDL4 script from the designated source\n" +
                "    \\RECORD; to start recording operators\n" +
                "    \\FLUSH [<file_expression>]; to stop recording (and optionally save it to designated file)\n");
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

    protected String getWelcomeText() {
        return "\n\n================================\n" +
                getExeName() + " REPL interactive\n" +
                "Type TDL4 statements to be executed in the REPL context in order of input, or a command.\n" +
                "Statement must always end with a semicolon. If not, it'll be continued on a next line.\n" +
                "If you want to type several statements at once on several lines, end each line with \\\n" +
                "Type \\QUIT; to end session and \\HELP; for list of all REPL commands\n";
    }

    protected String getExeName() {
        return "Data Cooker ETL";
    }

    protected String getReplPrompt() {
        return "datacooker";
    }

    public static void main(String[] args) {
        new Main().run(args);
    }

    public void run(String[] args) {
        Configuration config = new Configuration();

        JavaSparkContext context = null;
        try {
            config.setCommandLine(args, getExeName());

            SparkConf sparkConf = new SparkConf()
                    .setAppName(getExeName())
                    .set("spark.serializer", org.apache.spark.serializer.KryoSerializer.class.getCanonicalName());

            boolean repl = config.hasOption("repl");
            boolean local = repl || config.hasOption("local");
            if (local) {
                String cores = "*";
                if (config.hasOption("localCores")) {
                    cores = config.getOptionValue("localCores");
                }

                sparkConf
                        .setMaster("local[" + cores + "]")
                        .set("spark.network.timeout", "10000")
                        .set("spark.ui.enabled", String.valueOf(config.hasOption("sparkUI")));

                if (config.hasOption("driverMemory")) {
                    sparkConf.set("spark.driver.memory", config.getOptionValue("driverMemory"));
                }
            }

            context = new JavaSparkContext(sparkConf);
            context.hadoopConfiguration().set(FileInputFormat.INPUT_DIR_RECURSIVE, Boolean.TRUE.toString());

            if (repl) {
                Path historyPath = config.hasOption("history")
                        ? Path.of(config.getOptionValue("history"))
                        : Path.of(System.getProperty("user.home") + "/." + getReplPrompt() + ".history");

                Terminal terminal = TerminalBuilder.builder()
                        .nativeSignals(true)
                        .signalHandler(signal -> {
                            switch (signal) {
                                case INT: {
                                    throw new UserInterruptException(null);
                                }
                                case QUIT: {
                                    System.exit(0);
                                    break;
                                }
                                default: {
                                    throw new UnsupportedOperationException();
                                }
                            }
                        })
                        .build();
                LineReader reader = LineReaderBuilder.builder()
                        .parser(new DefaultParser().escapeChars(new char[0]).quoteChars(new char[0]))
                        .variable(LineReader.HISTORY_FILE, historyPath.toString())
                        .terminal(terminal)
                        .appName(getExeName() + " REPL")
                        .build();
                History history = new DefaultHistory();
                history.attach(reader);

                reader.printAbove(getWelcomeText());

                VariablesContext variablesContext = config.variables(context);
                variablesContext.put("CWD", Path.of("").toAbsolutePath().toString());
                VariablesContext options = new VariablesContext();
                options.put(DataContext.OPT_LOG_LEVEL, "WARN");
                DataContext dataContext = new DataContext(context);
                dataContext.initialize(options);

                boolean autoExec = config.hasOption("script");
                String line = autoExec ? "\\< '" + config.getOptionValue("script") + "';" : "";
                String cur;
                boolean contd = false, rec = false;
                StringBuilder record = new StringBuilder();
                Matcher matcher;
                String prompt, mainPr = getReplPrompt() + "> ", contdPr = StringUtils.leftPad("| ", getReplPrompt().length(), " ");
                while (true) {
                    try {
                        if (autoExec) {
                            autoExec = false;
                        } else {
                            if (!contd) {
                                line = "";
                            }

                            prompt = contd ? contdPr : mainPr;

                            cur = reader.readLine(prompt, rec ? "[R]" : null, (Character) null, null).trim();
                            line += cur;
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

                            matcher = LIST.matcher(line);
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
                                        reader.printAbove(HELP_TEXT.get("\\LIST"));
                                    }
                                }

                                continue;
                            }

                            matcher = EVAL.matcher(line);
                            if (matcher.matches()) {
                                String expr = matcher.group("expr");

                                TDL4ErrorListener errorListener = new TDL4ErrorListener();
                                TDL4Interpreter tdl4 = new TDL4Interpreter("", variablesContext, options, errorListener);
                                try {
                                    Object result = tdl4.interpretExpr(expr);
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
                                        Path flush = Path.of(expr);
                                        try {
                                            Files.writeString(flush, record);

                                            rec = false;
                                            record = new StringBuilder();
                                        } catch (Exception e) {
                                            reader.printAbove("Error while flushing the recording to '" + expr + "': " + e.getMessage());
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
                                TDL4Interpreter tdl4 = new TDL4Interpreter("", variablesContext, options, errorListener);
                                try {
                                    String path = String.valueOf(tdl4.interpretExpr(expr));

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
                            tdl4.initialize(dataContext);
                            tdl4.interpret();
                        }

                        if (rec) {
                            record.append(line).append("\n");
                        }
                    } catch (InvalidConfigurationException | IllegalArgumentException e) {
                        reader.printAbove(e.getMessage());
                    } catch (UserInterruptException e) {
                        line = "";
                        contd = false;
                    } catch (IOError | EndOfFileException e) {
                        break;
                    }
                }

                history.append(historyPath, true);
            } else {
                if (!config.hasOption("script")) {
                    LOG.error("No script to execute was specified");

                    System.exit(2);
                }

                String script = config.script(context, config.getOptionValue("script"));

                if (config.hasOption("dry")) {
                    TDL4ErrorListener errorListener = new TDL4ErrorListener();

                    new TDL4Interpreter(script, config.variables(context), new VariablesContext(), errorListener);

                    if (errorListener.errorCount > 0) {
                        List<String> errors = new ArrayList<>();
                        for (int i = 0; i < errorListener.errorCount; i++) {
                            errors.add("'" + errorListener.messages.get(i) + "' @ " + errorListener.lines.get(i) + ":" + errorListener.positions.get(i));
                        }

                        throw new InvalidConfigurationException("Invalid TDL4 script: " + errorListener.errorCount + " error(s).\n" +
                                String.join("\n", errors));
                    } else {
                        LOG.error("Input TDL4 script syntax check passed");
                    }
                } else {
                    final Map<String, Long> recordsRead = new HashMap<>();
                    final Map<String, Long> recordsWritten = new HashMap<>();

                    context.sc().addSparkListener(new SparkListener() {
                        @Override
                        public void onStageCompleted(SparkListenerStageCompleted stageCompleted) {
                            StageInfo stageInfo = stageCompleted.stageInfo();

                            long rR = stageInfo.taskMetrics().inputMetrics().recordsRead();
                            long rW = stageInfo.taskMetrics().outputMetrics().recordsWritten();
                            List<RDDInfo> infos = JavaConverters.seqAsJavaList(stageInfo.rddInfos());
                            List<String> rddNames = infos.stream()
                                    .map(RDDInfo::name)
                                    .filter(Objects::nonNull)
                                    .filter(n -> n.startsWith("datacooker:"))
                                    .collect(Collectors.toList());
                            if (rR > 0) {
                                rddNames.forEach(name -> recordsRead.compute(name, (n, r) -> (r == null) ? rR : rR + r));
                            }
                            if (rW > 0) {
                                rddNames.forEach(name -> recordsWritten.compute(name, (n, w) -> (w == null) ? rW : rW + w));
                            }
                        }
                    });

                    VariablesContext variables = config.variables(context);

                    TDL4ErrorListener errorListener = new TDL4ErrorListener();
                    TDL4Interpreter tdl4 = new TDL4Interpreter(script, variables, new VariablesContext(), errorListener);
                    if (errorListener.errorCount > 0) {
                        throw new InvalidConfigurationException("Invalid TDL4 script: " + errorListener.errorCount + " error(s). First error is '" + errorListener.messages.get(0)
                                + "' @ " + errorListener.lines.get(0) + ":" + errorListener.positions.get(0));
                    }

                    tdl4.initialize(new DataContext(context));
                    tdl4.interpret();

                    LOG.info("Raw physical record statistics");
                    recordsRead.forEach((key, value) -> LOG.info("Input '" + key + "': " + value + " record(s) read"));
                    recordsWritten.forEach((key, value) -> LOG.info("Output '" + key + "': " + value + " records(s) written"));
                }
            }
        } catch (Exception ex) {
            if (ex instanceof ParseException) {
                config.printHelp(getExeName());
            } else {
                LOG.error(ex.getMessage(), ex);
            }

            System.exit(1);
        } finally {
            if (context != null) {
                context.stop();
            }
        }
    }
}
