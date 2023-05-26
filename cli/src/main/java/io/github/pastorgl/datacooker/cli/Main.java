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
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.StageInfo;
import org.apache.spark.storage.RDDInfo;
import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.UserInterruptException;
import org.jline.reader.impl.DefaultParser;
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
    static final String CLI_NAME = "Data Cooker ETL";
    static final Pattern QUIT = Pattern.compile("(exit|quit|q|!).*", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    static final Pattern HELP = Pattern.compile("(help|h|\\?)(?:\\s+(?<cmd>.+))?", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    static final Pattern PRINT = Pattern.compile("(print|p|:)\\s+(?<ds>.+?)(?:\\s+(?<num>\\d+))?", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    static final Pattern EVAL = Pattern.compile("(eval|e|=)\\s+(?<expr>.+)", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    static final Pattern LIST = Pattern.compile("(show|list|l|\\|)\\s+(?<ent>.+)", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    static final Pattern SCRIPT = Pattern.compile("(script|source|s|<)\\s+(?<expr>.+)", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    private static final String WELCOME_TEXT = "\n\n================================\n" +
            CLI_NAME + " REPL interactive\n" +
            "Type any TDL4 statements to be executed in REPL context in order of input.\n" +
            "Statement must always end with a semicolon. If not, it'll be continued on a next line.\n" +
            "Type \\QUIT; to end session and \\HELP; for list of REPL commands\n";
    private static final Map<String, String> HELP_TEXT = new HashMap<>() {{
        put("", "Available REPL commands:\n" +
                "    \\QUIT; to end session\n" +
                "    \\HELP [\\COMMAND]; for \\COMMAND's help screen\n" +
                "    \\EVAL <TDL4_expression>; to evaluate a TDL4 expression\n" +
                "    \\PRINT <ds_name> [num_records]; to print a sample of num_records from data set ds_name\n" +
                "    \\SHOW <entity>; where entity is one of DS|Variable|Package|Operation|Transform|Input|Output\n" +
                "                    to list entities available in the current REPL session\n" +
                "    \\SCRIPT <file_expression>; to load and execute a TDL4 script from the designated file\n");
        put("\\QUIT", "\\QUIT;\n" +
                "    Ends current session and quits the REPL.\n" +
                "    Aliases: \\EXIT, \\Q, \\!");
        put("\\HELP", "\\HELP [\\COMMAND];\n" +
                "    Displays help on a selected \\COMMAND or lists all available commands.\n" +
                "    Aliases: \\H, \\?");
        put("\\EVAL", "\\EVAL <TDL4_expression>;\n" +
                "    Evaluates a TDL4 expression in the REPL context. Can reference any set $Variables.\n" +
                "    Aliases: \\E, \\=");
        put("\\PRINT", "\\PRINT <ds_name> [num_records];\n" +
                "    Samples random records from the referenced data set, and prints them.\n" +
                "    By default, 5 records are selected. Use TDL4 ANALYZE to retrieve number of records in a set\n" +
                "    Aliases: \\P, \\:");
        put("\\SHOW", "\\SHOW <entity>;\n" +
                "    List entities available in the current REPL session (only two first letters are significant):\n" +
                "        DS current Data Sets in the REPL context\n" +
                "        VAriables in the top-level REPL context\n" +
                "        PAckages in the classpath\n" +
                "        OPerations\n" +
                "        TRansforms\n" +
                "        INput|OUtput Storage Adapters\n" +
                "    Aliases: \\LIST, \\L, \\|");
        put("\\SCRIPT", "\\SCRIPT <file_expression>;\n" +
                "    Loads a script from the file which name is referenced by expression, evaluated to a String," +
                "    and executes it in REPL context\n" +
                "    Aliases: \\SOURCE, \\S, \\<");
    }};

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        Configuration config = new Configuration();

        JavaSparkContext context = null;
        try {
            config.setCommandLine(args, CLI_NAME);

            SparkConf sparkConf = new SparkConf()
                    .setAppName(CLI_NAME)
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
                LineReader reader = LineReaderBuilder.builder()
                        .parser(new DefaultParser().escapeChars(new char[0]).quoteChars(new char[0]))
                        .appName("Data Cooker REPL")
                        .build();

                reader.printAbove(WELCOME_TEXT);

                VariablesContext variablesContext = config.variables(context);
                variablesContext.put("CWD", Path.of("").toAbsolutePath().toString());
                VariablesContext options = new VariablesContext();
                options.put(DataContext.OPT_LOG_LEVEL, "WARN");
                DataContext dataContext = new DataContext(context);
                dataContext.initialize(options);
                String prompt, line = "", cur;
                boolean contd = false;
                Matcher matcher;
                while (true) {
                    try {
                        if (!contd) {
                            line = "";
                        }

                        prompt = contd ? "          | " : "datacooker> ";

                        cur = reader.readLine(prompt).trim();
                        line += cur;
                        if (cur.endsWith(";")) {
                            contd = false;
                        } else {
                            contd = true;
                            continue;
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

                                    line = Files.readString(Path.of(path));
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
                    } catch (InvalidConfigurationException | IllegalArgumentException e) {
                        reader.printAbove(e.getMessage());
                    } catch (UserInterruptException e) {
                        line = "";
                        contd = false;
                    } catch (IOError | EndOfFileException e) {
                        break;
                    }
                }
            } else {
                String script = config.script(context);

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
                config.printHelp(CLI_NAME);
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
