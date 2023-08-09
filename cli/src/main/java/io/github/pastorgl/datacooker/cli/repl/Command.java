/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.cli.repl;

import io.github.pastorgl.datacooker.metadata.DefinitionEnum;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public enum Command implements DefinitionEnum {
    QUIT(Pattern.compile("(quit|exit|q|!).*", Pattern.CASE_INSENSITIVE | Pattern.DOTALL),
            "\\QUIT;\n" +
            "    End current session and quit the REPL\n" +
            "    Aliases: \\EXIT, \\Q, \\!\n"),
    HELP(Pattern.compile("(help|h|\\?)(?:\\s+\\?(?<cmd>.+))?", Pattern.CASE_INSENSITIVE | Pattern.DOTALL),
            "\\HELP [\\COMMAND];\n" +
            "    Display help screen on a selected \\COMMAND or list all available commands\n" +
            "    Aliases: \\H, \\?\n"),
    EVAL(Pattern.compile("(eval|e|=)\\s+(?<expr>.+)", Pattern.CASE_INSENSITIVE | Pattern.DOTALL),
            "\\EVAL <TDL4_expression>;\n" +
                    "    Evaluate a TDL4 expression in the REPL context. Can reference any set $VARIABLES\n" +
                    "    Aliases: \\E, \\=\n"),
    PRINT(Pattern.compile("(print|p|:)\\s+(?<ds>.+?)(?:\\s+(?<num>\\d+))?", Pattern.CASE_INSENSITIVE | Pattern.DOTALL),
            "\\PRINT <ds_name> [num_records];\n" +
                    "    Sample random records from the referenced data set, and print them as key => value pairs.\n" +
                    "    By default, 5 records are selected. Use TDL4 ANALYZE to retrieve number of records in a set\n" +
                    "    Aliases: \\P, \\:\n"),
    SHOW(Pattern.compile("(show|list|l|\\|)\\s+(?<ent>.+)", Pattern.CASE_INSENSITIVE | Pattern.DOTALL),
            "\\SHOW <entity>;\n" +
                    "    List entities available in the current REPL session:\n" +
                    "        DS current Data Sets in the REPL context\n" +
                    "        VARIABLEs in the top-level REPL context\n" +
                    "        PACKAGEs in the classpath\n" +
                    "        TRANSFORMs\n" +
                    "        OPERATIONs\n" +
                    "        INPUT|OUTPUT Storage Adapters\n" +
                    "        OPTIONs of the REPL context\n" +
                    "    Aliases: \\LIST, \\L, \\|\n"),
    DESCRIBE(Pattern.compile("(describe|desc|d|!)\\s+(?<ent>.+?)\\s+(?<name>.+)", Pattern.CASE_INSENSITIVE | Pattern.DOTALL),
            "\\DESCRIBE <entity> <name>;\n" +
                    "    Provides a description of an entity referenced by its name. For list of entities, see \\SHOW\n" +
                    "    Aliases: \\DESC, \\D, \\!\n"),
    SCRIPT(Pattern.compile("(script|source|s|<)\\s+(?<expr>.+)", Pattern.CASE_INSENSITIVE | Pattern.DOTALL),
            "\\SCRIPT <source_expression>;\n" +
                    "    Load a script from the source which name is referenced by expression (evaluated to String),\n" +
                    "    parse, and execute it in REPL context operator by operator\n" +
                    "    Aliases: \\SOURCE, \\S, \\<\n"),
    RECORD(Pattern.compile("(record|start|r|\\[)", Pattern.CASE_INSENSITIVE | Pattern.DOTALL),
            "\\RECORD;\n" +
            "    Start recording TDL4 operators in the order of input. If operator execution ends with error,\n" +
            "    it won't be recorded. If you type multiple operators at once, they are recorded together\n" +
            "    Operators loaded by \\SCRIPT, if successful, will be recorded as a single chunk as well\n" +
            "    Aliases: \\START, \\R, \\[\n"),
    FLUSH(Pattern.compile("(flush|stop|f|])(:?\\s+(?<expr>.+))?", Pattern.CASE_INSENSITIVE | Pattern.DOTALL),
            "\\FLUSH [<file_expression>];\n" +
                    "    Stop recording. Save record to file which name is referenced by expression (evaluated to String)\n" +
                    "    Aliases: \\STOP, \\F, \\]\n")
    ;

    final public static String HELP_TEXT = "Available REPL commands:\n" +
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
            "    [!!] to repeat last line, [!n] to repeat n-th line from the last\n";

    final private Pattern pattern;
    final private String descr;

    Command(Pattern pattern, String descr) {
        this.pattern = pattern;
        this.descr = descr;
    }

    public static Command get(String cmd) {
        try {
            return valueOf(cmd.toUpperCase());
        } catch (Exception ignore) {
            return null;
        }
    }

    public Matcher matcher(CharSequence line) {
        return pattern.matcher(line);
    }

    @Override
    public String descr() {
        return descr;
    }
}
