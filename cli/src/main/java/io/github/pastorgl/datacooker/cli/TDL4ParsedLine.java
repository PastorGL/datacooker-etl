/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.cli;

import org.jline.reader.CompletingParsedLine;
import org.jline.reader.ParsedLine;

import java.util.List;

public class TDL4ParsedLine implements ParsedLine, CompletingParsedLine {
    private final String line;
    private final int cursor;

    public final List<String> words;
    public final Integer index;

    public TDL4ParsedLine(String line, int cursor, List<String> words, Integer index) {
        this.line = line;
        this.cursor = cursor;

        this.words = words;
        this.index = index;
    }

    @Override
    public CharSequence escape(CharSequence candidate, boolean complete) {
        return candidate;
    }

    @Override
    public int rawWordCursor() {
        if (index != null) {
            return cursor - words.subList(0, index).stream().map(String::length).reduce(0, Integer::sum);
        }

        return 0;
    }

    @Override
    public int rawWordLength() {
        return (index != null) ? words.get(index).length() : 0;
    }

    @Override
    public String word() {
        return (index != null) ? words.get(index) : "";
    }

    @Override
    public int wordCursor() {
        if (index != null) {
            return words.get(index).length();
        }

        return 0;
    }

    @Override
    public int wordIndex() {
        return (index != null) ? index : 0;
    }

    @Override
    public List<String> words() {
        return words;
    }

    @Override
    public String line() {
        return line;
    }

    @Override
    public int cursor() {
        return cursor;
    }
}
