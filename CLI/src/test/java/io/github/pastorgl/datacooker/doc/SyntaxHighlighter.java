package io.github.pastorgl.datacooker.doc;

import io.github.pastorgl.datacooker.scripting.TDL4Lexicon;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Token;

import static io.github.pastorgl.datacooker.scripting.TDL4.*;

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

        for (Token token : input.getTokens()) {
            String cls = null;

            switch (token.getType()) {
                case S_AMPERSAND:
                case S_BACKSLASH:
                case S_BACKTICK:
                case S_BANG:
                case S_CARET:
                case S_CLOSE_BRACKET:
                case S_CLOSE_PAR:
                case S_COLON:
                case S_COMMA:
                case S_CONCAT:
                case S_EQ:
                case S_GT:
                case S_HASH:
                case S_LT:
                case S_MINUS:
                case S_MINUSMINUS:
                case S_OPEN_BRACKET:
                case S_OPEN_PAR:
                case S_PERCENT:
                case S_PIPE:
                case S_PLUS:
                case S_QUESTION:
                case S_SCOL:
                case S_SLASH:
                case S_STAR:
                case S_TILDE:
                case K_REGEXP:
                case K_XOR:
                case K_AND:
                case K_BETWEEN:
                case K_CONCAT:
                case K_NOT:
                case K_IN:
                case K_IS:
                case K_OR:
                case K_DEFAULT:
                case K_DIGEST:
                case K_ARRAY: {
                    cls = "o";
                    break;
                }
                case K_UNION:
                case K_ANALYZE:
                case K_ANTI:
                case K_BEGIN:
                case K_CALL:
                case K_COLUMNS:
                case K_COPY:
                case K_CREATE:
                case K_DISTINCT:
                case K_DS:
                case K_ELSE:
                case K_END:
                case K_FROM:
                case K_IF:
                case K_INNER:
                case K_INPUT:
                case K_INTO:
                case K_JOIN:
                case K_KEY:
                case K_LEFT:
                case K_LET:
                case K_LIMIT:
                case K_LOOP:
                case K_OPTIONS:
                case K_OUTER:
                case K_OUTPUT:
                case K_RIGHT:
                case K_SELECT:
                case K_SET:
                case K_THEN:
                case K_WHERE:
                case K_TRANSFORM: {
                    cls = "s";
                    break;
                }
                case K_NULL: {
                    cls = "u";
                    break;
                }
                case K_FALSE:
                case K_TRUE: {
                    cls = "b";
                    break;
                }
                case K_VALUE:
                case T_POINT:
                case T_POLYGON:
                case T_SEGMENT:
                case T_TRACK: {
                    cls = "c";
                    break;
                }
                case S_DOT:
                case S_DOLLAR:
                case S_AT:
                case K_AS:
                case L_IDENTIFIER: {
                    cls = "i";
                    break;
                }
                case L_UNARY:
                case L_NUMERIC: {
                    cls = "n";
                    break;
                }
                case L_STRING: {
                    cls = "t";
                    break;
                }
                case L_COMMENT: {
                    cls = "m";
                    break;
                }
            }

            if (cls != null) {
                text.append("<c c=" + cls + ">" + token.getText() + "</c>");
            } else {
                text.append(token.getText());
            }
        }

        return text.toString();
    }
}
