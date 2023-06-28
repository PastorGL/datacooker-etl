/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.scripting;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;

import java.util.ArrayList;
import java.util.List;

public class TDL4ErrorListener extends BaseErrorListener {
    public int errorCount = 0;
    public final List<String> messages = new ArrayList<>();
    public final List<Integer> lines = new ArrayList<>();
    public final List<Integer> positions = new ArrayList<>();

    @Override
    public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String msg, RecognitionException e) {
        messages.add(msg);
        lines.add(line);
        positions.add(charPositionInLine);

        errorCount++;
    }
}
