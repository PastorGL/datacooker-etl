/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.cli.repl;

import io.github.pastorgl.datacooker.metadata.DescribedEnum;

import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public enum Command implements DescribedEnum {
    QUIT(Pattern.compile("(quit|exit|q|!).*", Pattern.CASE_INSENSITIVE | Pattern.DOTALL),
            "\\QUIT; to end session",
            """
                    \\QUIT;
                        End current session and quit the REPL
                        Aliases: \\EXIT, \\Q, \\!
                    """),
    HELP(Pattern.compile("(help|h|\\?)(?:\\s+(?<cmd>.+))?", Pattern.CASE_INSENSITIVE | Pattern.DOTALL),
            "\\HELP [\\COMMAND]; for \\COMMAND's help screen",
            """
                    \\HELP [\\COMMAND];
                        Display help screen on a selected \\COMMAND or list all available commands
                        Aliases: \\H, \\?
                    """),
    EVAL(Pattern.compile("(eval|e|=)\\s+(?<expr>.+)", Pattern.CASE_INSENSITIVE | Pattern.DOTALL),
            "\\EVAL <TDL_expression>; to evaluate a TDL expression",
            """
                    \\EVAL <TDL_expression>;
                        Evaluate a TDL expression in the REPL context. Can reference any set $VARIABLES
                        Aliases: \\E, \\=
                    """),
    PRINT(Pattern.compile("(print|p|:)\\s+(?<ds>.+?)(?:\\s+(?<i1>\\d+))?(?:\\s+(?<i2>\\d+))?", Pattern.CASE_INSENSITIVE | Pattern.DOTALL),
            "\\PRINT <ds_name> [[part] limit]; to print a sample of records from data set ds_name",
            """
                    \\PRINT <ds_name> [[part] limit];
                        Print data set records as key => value pairs up to set limit. If part number is specified, take
                        first records from that part. Otherwise sample random records from the referenced data set.
                        By default, 5 random records are selected. Use ANALYZE statement to retrieve number of records
                        and/or \\DESCRIBE to get number of parts
                        Aliases: \\P, \\:
                    """),
    RENOUNCE(Pattern.compile("(renounce|n|-)\\s+(?<ds>.+)", Pattern.CASE_INSENSITIVE | Pattern.DOTALL),
            "\\RENOUNCE <ds_name>; to free DS ds_name for another use. DS itself will be left intact",
            """
                    \\RENOUNCE <ds_name>;
                        Free a data set name, allowing it to be reused for another data set. The initial data set
                        remains in the context, but becomes inaccessible
                        Aliases: \\N, \\-
                    """),
    PERSIST(Pattern.compile("(persist|cache|c|\\+)\\s+(?<ds>.+)", Pattern.CASE_INSENSITIVE | Pattern.DOTALL),
            "\\PERSIST <ds_name>; to cache DS ds_name in the context's persistent storage",
            """
                    \\PERSIST <ds_name>;
                        Cache a data set (by increasing its usage count to usage threshold)
                        Aliases: \\CACHE, \\C, \\+
                    """),
    LINEAGE(Pattern.compile("(lineage|g|^)\\s+(?<ds>.+)", Pattern.CASE_INSENSITIVE | Pattern.DOTALL),
            "\\LINEAGE <ds_name>; to show DS ds_name ancestors",
            """
                    \\LINEAGE <ds_name>;
                        Show data set ancestors and their complete history of changes
                        Aliases: \\G, \\^
                    """),
    SHOW(Pattern.compile("(show|list|l|\\|)\\s+(?<ent>.+)", Pattern.CASE_INSENSITIVE | Pattern.DOTALL),
            """
                    \\SHOW <entity>; where entity is one of DS|Variable|Package|Operation|Transform
                                        |Input|Output|Function|Procedure
                                        to list entities available in the current REPL session""",
            """
                    \\SHOW <entity>;
                        List entities available in the current REPL session:
                            DS current Data Sets in the REPL context
                            VARIABLEs in the top-level REPL context
                            PACKAGEs in the classpath
                            TRANSFORMs
                            OPERATIONs
                            INPUT|OUTPUT Storage Adapters
                            OPTIONs of the REPL context
                            OPERATORs
                            FUNCTIONs
                            PROCEDUREs
                        Aliases: \\LIST, \\L, \\|
                    """),
    DESCRIBE(Pattern.compile("(describe|desc|d|;)\\s+(?<ent>.+?)\\s+(?<name>.+)", Pattern.CASE_INSENSITIVE | Pattern.DOTALL),
            "\\DESCRIBE <entity> <name>; to describe an entity referenced by its name",
            """
                    \\DESCRIBE <entity> <name>;
                        Provide a description of an entity referenced by its name. For list of entities, see \\SHOW
                        Aliases: \\DESC, \\D, \\;
                    """),
    SCRIPT(Pattern.compile("(script|source|s|<)\\s+(?<expr>.+?)(?:\\s+(?<dry>-dry))?", Pattern.CASE_INSENSITIVE | Pattern.DOTALL),
            "\\SCRIPT <source_expression> [-dry]; to load and execute script(s) from the designated source",
            """
                    \\SCRIPT <source_expression> [-dry];
                        Load script(s) from the glob patterned path defined by expression (evaluated to String),
                        parse, and if not -dry, execute it in REPL context operator by operator
                        Aliases: \\SOURCE, \\S, \\<
                    """),
    RECORD(Pattern.compile("(record|start|r|\\[)", Pattern.CASE_INSENSITIVE | Pattern.DOTALL),
            "\\RECORD; to start recording operators",
            """
                    \\RECORD;
                        Start recording TDL operators in the order of input. If operator execution ends with error,
                        it won't be recorded. If you type multiple operators at once, they are recorded together
                        Operators loaded by \\SCRIPT, if successful, will be recorded as a single chunk as well
                        Aliases: \\START, \\R, \\[
                    """),
    FLUSH(Pattern.compile("(flush|stop|f|])(:?\\s+(?<expr>.+))?", Pattern.CASE_INSENSITIVE | Pattern.DOTALL),
            "\\FLUSH [<file_expression>]; to stop recording (and optionally save it to designated file)",
            """
                    \\FLUSH [<file_expression>];
                        Stop recording. Save record to file which name is referenced by expression (evaluated to String)
                        Aliases: \\STOP, \\F, \\]
                    """);

    final public static String HELP_TEXT = "Available REPL commands:\n" +
            Arrays.stream(Command.values()).map(c -> c.descr).collect(Collectors.joining("\n", "", "\n")) +
            """
                    Available shortcuts:
                    [Ctrl C] then [Enter] to abort currently unfinished line(s)
                    [Ctrl D] to abort input
                    [Ctrl R] to reverse search in history
                    [Up] and [Down] to scroll through history
                    [!!] to repeat last line, [!n] to repeat n-th line from the last
                    """;

    final private Pattern pattern;
    final private String descr;
    final private String full;

    Command(Pattern pattern, String descr, String full) {
        this.pattern = pattern;
        this.descr = descr;
        this.full = full;
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
        return full;
    }
}
