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
            ""),
    HELP(Pattern.compile("(help|h|\\?)(?:\\s+(?<cmd>.+))?", Pattern.CASE_INSENSITIVE | Pattern.DOTALL),
            "\\HELP [\\COMMAND]; for \\COMMAND's help screen",
            ""),
    EVAL(Pattern.compile("(eval|e|=)\\s+(?<expr>.+)", Pattern.CASE_INSENSITIVE | Pattern.DOTALL),
            "\\EVAL <TDL_expression>; to evaluate a TDL expression",
            ""),
    PRINT(Pattern.compile("(print|p|:)\\s+(?<ds>.+?)(?:\\s+(?<i1>\\d+))?(?:\\s+(?<i2>\\d+))?", Pattern.CASE_INSENSITIVE | Pattern.DOTALL),
            "\\PRINT <ds_name> [[part] limit]; to print a sample of records from data set ds_name",
            ""),
    RENOUNCE(Pattern.compile("(renounce|n|-)\\s+(?<ds>.+)", Pattern.CASE_INSENSITIVE | Pattern.DOTALL),
            "\\RENOUNCE <ds_name>; to free DS ds_name for another use. DS itself will be left intact",
            ""),
    PERSIST(Pattern.compile("(persist|cache|c|\\+)\\s+(?<ds>.+)", Pattern.CASE_INSENSITIVE | Pattern.DOTALL),
            "\\PERSIST <ds_name>; to cache DS ds_name in the context's persistent storage",
            ""),
    LINEAGE(Pattern.compile("(lineage|g|^)\\s+(?<ds>.+)", Pattern.CASE_INSENSITIVE | Pattern.DOTALL),
            "\\LINEAGE <ds_name>; to show DS ds_name ancestors",
            ""),
    SHOW(Pattern.compile("(show|list|l|\\|)\\s+(?<ent>.+)", Pattern.CASE_INSENSITIVE | Pattern.DOTALL),
            "",
            ""),
    DESCRIBE(Pattern.compile("(describe|desc|d|;)\\s+(?<ent>.+?)\\s+(?<name>.+)", Pattern.CASE_INSENSITIVE | Pattern.DOTALL),
            "\\DESCRIBE <entity> <name>; to describe an entity referenced by its name",
            ""),
    SCRIPT(Pattern.compile("(script|source|s|<)\\s+(?<expr>.+?)(?:\\s+(?<dry>-dry))?", Pattern.CASE_INSENSITIVE | Pattern.DOTALL),
            "\\SCRIPT <source_expression> [-dry]; to load and execute script(s) from the designated source",
            ""),
    RECORD(Pattern.compile("(record|start|r|\\[)", Pattern.CASE_INSENSITIVE | Pattern.DOTALL),
            "\\RECORD; to start recording operators",
            ""),
    FLUSH(Pattern.compile("(flush|stop|f|])(:?\\s+(?<expr>.+))?", Pattern.CASE_INSENSITIVE | Pattern.DOTALL),
            "\\FLUSH [<file_expression>]; to stop recording (and optionally save it to designated file)",
            "");

    final public static String HELP_TEXT = "Available REPL commands:\n" +
            Arrays.stream(Command.values()).map(c -> c.descr).collect(Collectors.joining("\n", "", "\n")) +
            "";

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
