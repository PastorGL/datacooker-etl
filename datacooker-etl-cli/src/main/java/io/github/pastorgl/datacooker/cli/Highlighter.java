/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.cli;

import io.github.pastorgl.datacooker.scripting.Highlight;
import io.github.pastorgl.datacooker.scripting.TDL4Lexicon;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Token;

public class Highlighter {
    private final CommonTokenStream input;

    public Highlighter(String script) {
        CharStream cs = CharStreams.fromString(script);
        TDL4Lexicon lexer = new TDL4Lexicon(cs);
        input = new CommonTokenStream(lexer);
    }

    public String highlight() {
        StringBuilder text = new StringBuilder();
        input.fill();

        String cls;
        Highlight highlight;
        for (Token token : input.getTokens()) {
            highlight = Highlight.get(token.getType());
            if (highlight == null) {
                text.append(token.getText());
            } else {
                cls = switch (highlight) {
                    case OPERATOR -> "o";
                    case KEYWORD -> "s";
                    case NULL -> "u";
                    case BOOLEAN -> "b";
                    case TYPE -> "c";
                    case IDENTIFIER -> "i";
                    case SIGIL -> "g";
                    case NUMERIC -> "n";
                    case STRING -> "t";
                    case COMMENT -> "m";
                };

                text.append("<c c=").append(cls).append(">").append(token.getText()).append("</c>");
            }
        }

        return text.toString();
    }
}
