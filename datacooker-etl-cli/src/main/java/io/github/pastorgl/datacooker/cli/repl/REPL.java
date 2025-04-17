/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.cli.repl;

import io.github.pastorgl.datacooker.Options;
import io.github.pastorgl.datacooker.PackageInfo;
import io.github.pastorgl.datacooker.cli.Configuration;
import io.github.pastorgl.datacooker.data.StreamLineage;
import io.github.pastorgl.datacooker.metadata.*;
import io.github.pastorgl.datacooker.scripting.*;
import org.apache.commons.lang3.StringUtils;
import org.jline.reader.EndOfFileException;
import org.jline.reader.History;
import org.jline.reader.LineReader;
import org.jline.reader.impl.history.DefaultHistory;
import org.jline.terminal.TerminalBuilder;

import java.io.IOError;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;

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
                """
                        Type TDL statements to be executed in the REPL context in order of input, or a command.
                        Statement must always end with a semicolon. If not, it'll be continued on a next line.
                        If you want to type several statements at once on several lines, end each line with \\
                        Type \\QUIT; to end session and \\HELP; for list of all REPL commands and shortcuts
                        """;
    }

    public void loop() throws Exception {
        Path historyPath = config.hasOption("history")
                ? Path.of(config.getOptionValue("history"))
                : Path.of(System.getProperty("user.home") + "/." + replPrompt + ".history");

        ReplCompleter completer = new ReplCompleter(vp, dp, ep, op, exp);
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

        boolean dry;
        if (config.hasOption("script")) {
            String path = config.getOptionValue("script");

            reader.printAbove("Parsing command line script(s) '" + path + "'");
            String script = exp.readDirect(path);

            if (noErrors(script, reader)) {
                dry = config.hasOption("dry");
                if (!dry) {
                    reader.printAbove("Executing command line script(s) '" + path + "'");
                    exp.interpret(script);
                }
            }
        }

        String cur, line = "";
        boolean contd = false, rec = false;
        StringBuilder recorder = new StringBuilder();
        Matcher matcher;
        String prompt, mainPr = replPrompt + "> ", contdPr = StringUtils.leftPad("| ", mainPr.length(), " ");
        while (true) {
            dry = false;
            try {
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
                            cmd = cmd.startsWith("\\") ? cmd.substring(1) : cmd;

                            Command c = get(cmd);
                            if (c != null) {
                                reader.printAbove(c.descr());

                                continue;
                            }
                        }

                        reader.printAbove(HELP_TEXT);
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
                            if ("OPERATORS".startsWith(ent)) {
                                reader.printAbove(String.join(", ", ep.getAllOperators()) + "\n");
                                break show;
                            }
                            if ("FUNCTIONS".startsWith(ent)) {
                                reader.printAbove(String.join(", ", ep.getAllFunctions()) + "\n");
                                break show;
                            }
                            if ("PROCEDURES".startsWith(ent)) {
                                reader.printAbove(String.join(", ", exp.getAllProcedures()) + "\n");
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

                                    OptionsInfo uti = op.get(Options.usage_threshold.name());
                                    String ut = (uti.value == null) ? uti.def : uti.value;
                                    reader.printAbove(ds.describe(ut));
                                }
                                break desc;
                            }
                            if ("VARIABLES".startsWith(ent)) {
                                String vi = vp.getVar(name).describe();
                                reader.printAbove(vi);

                                break desc;
                            }
                            if ("PACKAGES".startsWith(ent)) {
                                if (ep.hasPackage(name)) {
                                    PackageInfo pi = ep.getPackage(name);
                                    reader.printAbove(pi.descr + "\n");
                                    reader.printAbove(pi.pluggables.size() + " Pluggables\n");
                                    reader.printAbove(pi.operators.size() + " Operators\n");
                                    reader.printAbove(pi.functions.size() + " Functions\n");
                                }
                                break desc;
                            }
                            if ("TRANSFORMS".startsWith(ent)) {
                                if (ep.hasTransform(name)) {
                                    PluggableMeta meta = ep.getTransform(name);

                                    StringBuilder sb = new StringBuilder();
                                    sb.append(meta.descr + "\n");
                                    sb.append("Keying: " + (meta.dsFlag(DSFlag.KEY_BEFORE) ? "before" : "after") + " transform\n");

                                    sb.append("Input:\n");
                                    describeStreams(meta.input, sb);

                                    describeDefinitions(meta, sb);

                                    sb.append("Output:\n");
                                    describeStreams(meta.output, sb);

                                    reader.printAbove(sb.toString());
                                }
                                break desc;
                            }
                            if ("OPERATIONS".startsWith(ent)) {
                                if (ep.hasOperation(name)) {
                                    PluggableMeta meta = ep.getOperation(name);

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
                                    PluggableMeta meta = ep.getInput(name);

                                    StringBuilder sb = new StringBuilder();
                                    sb.append(meta.descr + "\n");

                                    describeStreams(meta.input, sb);

                                    describeDefinitions(meta, sb);

                                    sb.append("Produces:\n");
                                    describeStreams(meta.output, sb);

                                    reader.printAbove(sb.toString());
                                }
                                break desc;
                            }
                            if ("OUTPUT".startsWith(ent)) {
                                if (ep.hasOutput(name)) {
                                    PluggableMeta meta = ep.getOutput(name);

                                    StringBuilder sb = new StringBuilder();
                                    sb.append(meta.descr + "\n");

                                    sb.append("Consumes:\n");
                                    describeStreams(meta.input, sb);

                                    describeDefinitions(meta, sb);

                                    describeStreams(meta.output, sb);

                                    reader.printAbove(sb.toString());
                                }
                                break desc;
                            }
                            if ("OPTIONS".startsWith(ent)) {
                                OptionsInfo oi = op.get(name);

                                if (oi != null) {
                                    StringBuilder sb = new StringBuilder();
                                    sb.append(oi.descr + "\n");
                                    sb.append("Type: " + oi.clazz + "\n");
                                    sb.append("Default: " + oi.def + "\n");
                                    sb.append("Current: " + oi.value + "\n");

                                    reader.printAbove(sb.toString());
                                }
                                break desc;
                            }
                            if ("OPERATORS".startsWith(ent)) {
                                OperatorInfo ei = ep.getOperator(name);

                                if (ei != null) {
                                    StringBuilder sb = new StringBuilder();
                                    sb.append(ei.descr + "\n");
                                    sb.append("\tReturns: " + ei.resultType + "\n");
                                    sb.append("\t" + ei.arity + " operand(s): " + String.join(", ", ei.argTypes) + "\n");
                                    sb.append("\tPriority " + ei.priority + (ei.rightAssoc ? ", right associative" : "") + (ei.handleNull ? ", handles NULLs" : "") + "\n");

                                    reader.printAbove(sb.toString());
                                }
                                break desc;
                            }
                            if ("FUNCTIONS".startsWith(ent)) {
                                FunctionInfo ei = ep.getFunction(name);

                                if (ei != null) {
                                    StringBuilder sb = new StringBuilder();
                                    sb.append(ei.descr + "\n");
                                    sb.append("\tReturns: " + ei.resultType + "\n");
                                    switch (ei.arity) {
                                        case Function.RECORD_LEVEL: {
                                            sb.append("\tRecord level with explicit arguments\n");
                                            break;
                                        }
                                        case Function.WHOLE_RECORD: {
                                            sb.append("\tImplicit Record Key and Object: " + ei.argTypes[0] + "; explicit arguments\n");
                                            break;
                                        }
                                        case Function.RECORD_KEY: {
                                            sb.append("\tImplicit Record Key; explicit arguments\n");
                                            break;
                                        }
                                        case Function.RECORD_OBJECT: {
                                            sb.append("\tImplicit Record Object: " + ei.argTypes[0] + "; explicit arguments\n");
                                            break;
                                        }
                                        case Function.ARBITR_ARY: {
                                            sb.append("\tExplicit arguments: " + ei.argTypes[0] + ". See description for details\n");
                                            break;
                                        }
                                        case Function.NO_ARGS: {
                                            sb.append("\tNo arguments\n");
                                            break;
                                        }
                                        default: {
                                            sb.append("\t" + ei.arity + " argument(s): " + String.join(", ", ei.argTypes) + "\n");
                                        }
                                    }

                                    reader.printAbove(sb.toString());
                                }
                                break desc;
                            }
                            if ("PROCEDURES".startsWith(ent)) {
                                Map<String, Param> params = exp.getProcedure(name);
                                if (params != null) {
                                    StringBuilder sb = new StringBuilder();

                                    if (!params.isEmpty()) {
                                        sb.append("Parameters:\n");
                                        for (Map.Entry<String, Param> def : params.entrySet()) {
                                            Param val = def.getValue();
                                            if (val.optional) {
                                                sb.append("Optional " + def.getKey() + " = " + val.defaults + "\n");
                                            } else {
                                                sb.append("Mandatory " + def.getKey() + "\n");
                                            }
                                        }
                                    }

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

                            reader.printAbove(new VariableInfo(result).describe());
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
                        String ds = unescapeId(matcher.group("ds"));

                        if (dp.has(ds)) {
                            String i1 = matcher.group("i1");
                            String i2 = matcher.group("i2");

                            int limit = 5;
                            if (i2 != null) {
                                int part = Utils.parseNumber(i1).intValue();
                                limit = Utils.parseNumber(i2).intValue();

                                dp.part(ds, part, limit)
                                        .forEach(r -> reader.printAbove(r + "\n"));
                            } else {
                                if (i1 != null) {
                                    limit = Utils.parseNumber(i1).intValue();
                                }

                                dp.sample(ds, limit)
                                        .forEach(r -> reader.printAbove(r + "\n"));
                            }
                        } else {
                            reader.printAbove("There is no DS named '" + ds + "'\n");
                        }

                        continue;
                    }

                    matcher = PERSIST.matcher(line);
                    if (matcher.matches()) {
                        String dsName = unescapeId(matcher.group("ds"));

                        if (dp.has(dsName)) {
                            StreamInfo ds = dp.persist(dsName);

                            OptionsInfo uti = op.get(Options.usage_threshold.name());
                            String ut = (uti.value == null) ? uti.def : uti.value;
                            reader.printAbove(ds.describe(ut));
                        } else {
                            reader.printAbove("There is no DS named '" + dsName + "'\n");
                        }

                        continue;
                    }

                    matcher = RENOUNCE.matcher(line);
                    if (matcher.matches()) {
                        String dsName = unescapeId(matcher.group("ds"));

                        if (dp.has(dsName)) {
                            dp.renounce(dsName);
                        } else {
                            reader.printAbove("There is no DS named '" + dsName + "'\n");
                        }

                        continue;
                    }

                    matcher = LINEAGE.matcher(line);
                    if (matcher.matches()) {
                        String dsName = unescapeId(matcher.group("ds"));

                        if (dp.has(dsName)) {
                            for (StreamLineage sl : dp.lineage(dsName)) {
                                reader.printAbove(sl.toString() + "\n");
                            }
                        } else {
                            reader.printAbove("There is no DS named '" + dsName + "'\n");
                        }

                        continue;
                    }

                    // This must be last command case. If good, evaluation continues past command if line.startsWith('\\')
                    matcher = SCRIPT.matcher(line);
                    if (matcher.matches()) {
                        String expr = matcher.group("expr");
                        dry = matcher.group("dry") != null;

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

                if (noErrors(line, reader)) {
                    if (!dry) {
                        exp.interpret(line);
                    }
                } else {
                    continue;
                }

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

    private boolean noErrors(String script, ReplLineReader reader) {
        TDLErrorListener errorListener = exp.parse(script);

        boolean hasErrors = errorListener.errorCount > 0;
        if (hasErrors) {
            List<String> errors = new ArrayList<>();
            for (int i = 0; i < errorListener.errorCount; i++) {
                errors.add("'" + errorListener.messages.get(i) + "' @ " + errorListener.lines.get(i) + ":" + errorListener.positions.get(i));
            }

            reader.printAbove(errorListener.errorCount + " error(s).\n" +
                    String.join("\n", errors));
        }

        return !hasErrors;
    }

    private static void describeStreams(InputOutputMeta meta, StringBuilder sb) {
        if (meta instanceof PathMeta) {
            sb.append("Path examples:\n\t" + String.join("\n\t", ((PathMeta) meta).examples) + "\n");

            return;
        }

        Map streams = null;
        if (meta instanceof InputMeta || meta instanceof NamedInputMeta) {
            streams = meta instanceof NamedInputMeta
                    ? ((NamedInputMeta) meta).streams : Map.of("", (InputMeta) meta);
        }

        if (meta instanceof OutputMeta || meta instanceof NamedOutputMeta) {
            streams = meta instanceof NamedOutputMeta
                    ? ((NamedOutputMeta) meta).streams : Map.of("", (OutputMeta) meta);
        }

        for (Object e : streams.entrySet()) {
            String name = (String) ((Map.Entry) e).getKey();
            Object stream = ((Map.Entry) e).getValue();

            InputMeta input = (InputMeta) stream;
            sb.append("\t");
            if (stream instanceof OutputMeta output) {
                if (output.origin != null) {
                    sb.append(output.origin + " ");
                }
            }
            if (!name.isEmpty()) {
                sb.append((input.optional ? "Optional" : "Mandatory") + " \"" + name + "\": ");
            }
            sb.append(input.type + "\n");
            sb.append("\t\t" + input.descr + "\n");

            if (stream instanceof OutputMeta output) {
                List<String> anc = output.ancestors;
                if (anc != null) {
                    sb.append("\t\tAncestors: \"" + String.join("\", \"", anc) + "\"\n");
                }
                Map<String, String> gen = output.generated;
                if ((gen != null) && !gen.isEmpty()) {
                    sb.append("\t\tAttributes:\n");
                    gen.forEach((key, value) -> sb.append("\t\t\t" + key + " " + value + "\n"));
                }
            }
        }
    }

    private static void describeDefinitions(PluggableMeta meta, StringBuilder sb) {
        if (meta.definitions != null) {
            sb.append("Parameters:\n");
            for (Map.Entry<String, DefinitionMeta> def : meta.definitions.entrySet()) {
                DefinitionMeta val = def.getValue();
                if (val.optional) {
                    sb.append("\tOptional @" + def.getKey() + ": " + val.friendlyType + " = " + val.defaults + " (" + val.defDescr + ")\n");
                } else if (val.dynamic) {
                    sb.append("\tDynamic @" + def.getKey() + ": " + val.friendlyType + "\n");
                } else {
                    sb.append("\tMandatory @" + def.getKey() + ": " + val.friendlyType + "\n");
                }
                sb.append("\t\t" + val.descr + "\n");
                if (val.enumValues != null) {
                    val.enumValues.entrySet().forEach(e -> sb.append("\t\t" + e.getKey() + " " + e.getValue() + "\n"));
                }
            }
        } else {
            sb.append("No parameters\n");
        }
    }
}
