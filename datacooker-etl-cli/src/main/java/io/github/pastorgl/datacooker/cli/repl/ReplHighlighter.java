/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.cli.repl;

import io.github.pastorgl.datacooker.scripting.Highlight;
import io.github.pastorgl.datacooker.scripting.TDLErrorListener;
import io.github.pastorgl.datacooker.scripting.TDLLexicon;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Token;
import org.apache.commons.lang3.StringUtils;
import org.jline.reader.Highlighter;
import org.jline.reader.LineReader;
import org.jline.utils.AttributedString;
import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.AttributedStyle;

import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class ReplHighlighter implements Highlighter {
    @Override
    public AttributedString highlight(LineReader reader, String buffer) {
        AttributedStringBuilder sb = new AttributedStringBuilder();

        boolean errors = false;

        if (buffer.startsWith("\\")) {
            int index = buffer.indexOf(" ");
            if (index < 0) {
                errors = true;
                index = buffer.length();
            }
            sb.append(new AttributedString(buffer.substring(0, index), new AttributedStyle().foreground(AttributedStyle.CYAN + AttributedStyle.BRIGHT)));
            if (index > 0) {
                buffer = buffer.substring(index);
            }
        }

        int errorPos = buffer.length();
        if (!errors) {
            TDLErrorListener errorListener = new TDLErrorListener();

            TDLLexicon lexer = new TDLLexicon(CharStreams.fromString(buffer));
            lexer.removeErrorListeners();
            lexer.addErrorListener(errorListener);
            CommonTokenStream stream = new CommonTokenStream(lexer);
            stream.fill();

            List<Token> tokens = stream.getTokens().stream()
                    .filter(t -> t.getType() != TDLLexicon.EOF)
                    .collect(Collectors.toList());

            errors = errorListener.errorCount > 0;
            if (errors) {
                errorPos = errorListener.positions.get(0);
            }

            for (Token token : tokens) {
                if (errors && (errorPos < token.getStartIndex())) {
                    break;
                }

                Highlight highlight = Highlight.get(token.getType());
                String word = token.getText();
                if (highlight == null) {
                    sb.append(new AttributedString(word, AttributedStyle.DEFAULT));
                } else {
                    switch (highlight) {
                        case OPERATOR: {
                            sb.append(new AttributedString(word.toUpperCase(), AttributedStyle.DEFAULT));
                            break;
                        }
                        case KEYWORD: {
                            sb.append(new AttributedString(word.toUpperCase(), AttributedStyle.BOLD));
                            break;
                        }
                        case NULL:
                        case BOOLEAN: {
                            sb.append(new AttributedString(word.toUpperCase(), new AttributedStyle().foreground(AttributedStyle.MAGENTA + AttributedStyle.BRIGHT)));
                            break;
                        }
                        case TYPE: {
                            sb.append(new AttributedString(StringUtils.capitalize(word), new AttributedStyle().foreground(AttributedStyle.BLUE + AttributedStyle.BRIGHT)));
                            break;
                        }
                        case IDENTIFIER: {
                            sb.append(new AttributedString(word, new AttributedStyle().foreground(AttributedStyle.YELLOW)));
                            break;
                        }
                        case SIGIL: {
                            sb.append(new AttributedString(word, new AttributedStyle().foreground(AttributedStyle.YELLOW + AttributedStyle.BRIGHT)));
                            break;
                        }
                        case NUMERIC: {
                            sb.append(new AttributedString(word.toUpperCase(), new AttributedStyle().foreground(AttributedStyle.GREEN + AttributedStyle.BRIGHT)));
                            break;
                        }
                        case STRING: {
                            sb.append(new AttributedString(word, new AttributedStyle().foreground(AttributedStyle.MAGENTA).bold()));
                            break;
                        }
                        case COMMENT: {
                            sb.append(new AttributedString(word, new AttributedStyle().foreground(AttributedStyle.WHITE).italic()));
                            break;
                        }
                    }
                }
            }
        }

        if (errors && (errorPos < buffer.length())) {
            sb.append(new AttributedString(buffer.substring(errorPos), new AttributedStyle().foreground(AttributedStyle.RED + AttributedStyle.BRIGHT)));
        }

        return sb.toAttributedString();
    }

    @Override
    public void setErrorPattern(Pattern errorPattern) {
    }

    @Override
    public void setErrorIndex(int errorIndex) {
    }
}
