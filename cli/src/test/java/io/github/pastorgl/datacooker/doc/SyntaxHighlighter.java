/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.doc;

import io.github.pastorgl.datacooker.scripting.Highlight;
import io.github.pastorgl.datacooker.scripting.TDL4Lexicon;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Token;

public class SyntaxHighlighter {
    private final CommonTokenStream input;

    public SyntaxHighlighter(String script) {
        CharStream cs = CharStreams.fromString(script);
        TDL4Lexicon lexer = new TDL4Lexicon(cs);
        input = new CommonTokenStream(lexer);
    }

    public String highlight() {
        StringBuilder text = new StringBuilder();
        input.fill();

        String cls = null;
        Highlight highlight;
        for (Token token : input.getTokens()) {
            highlight = Highlight.get(token.getType());
            if (highlight == null) {
                text.append(token.getText());
            } else {
                switch (highlight) {
                    case OPERATOR: {
                        cls = "o";
                        break;
                    }
                    case KEYWORD: {
                        cls = "s";
                        break;
                    }
                    case NULL: {
                        cls = "u";
                        break;
                    }
                    case BOOLEAN: {
                        cls = "b";
                        break;
                    }
                    case OBJLVL: {
                        cls = "c";
                        break;
                    }
                    case IDENTIFIER: {
                        cls = "i";
                        break;
                    }
                    case SIGIL: {
                        cls = "g";
                        break;
                    }
                    case NUMERIC: {
                        cls = "n";
                        break;
                    }
                    case STRING: {
                        cls = "t";
                        break;
                    }
                    case COMMENT: {
                        cls = "m";
                        break;
                    }
                }

                text.append("<c c=").append(cls).append(">").append(token.getText()).append("</c>");
            }
        }

        return text.toString();
    }
}
