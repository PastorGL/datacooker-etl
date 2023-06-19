package io.github.pastorgl.datacooker.cli;

import io.github.pastorgl.datacooker.data.DataContext;
import io.github.pastorgl.datacooker.data.DataStream;
import io.github.pastorgl.datacooker.data.Transforms;
import io.github.pastorgl.datacooker.scripting.VariablesContext;
import org.antlr.v4.runtime.Token;
import org.jline.reader.Candidate;
import org.jline.reader.Completer;
import org.jline.reader.LineReader;
import org.jline.reader.ParsedLine;

import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import static io.github.pastorgl.datacooker.Constants.OBJLVL_VALUE;
import static io.github.pastorgl.datacooker.scripting.TDL4.*;

public class TDL4Completer implements Completer {
    private final VariablesContext vars;
    private final DataContext data;

    private final Set<Integer> COMPL_STMT = Set.of(K_CREATE, K_TRANSFORM, K_COPY, K_LET, K_LOOP, K_IF, K_SELECT, K_CALL, K_ANALYZE, K_OPTIONS);
    private final Pattern ID_PATTERN = Pattern.compile("[a-z_][a-z_0-9.]*", Pattern.CASE_INSENSITIVE);

    public TDL4Completer(VariablesContext variablesContext, DataContext dataContext) {
        vars = variablesContext;
        data = dataContext;
    }

    @Override
    public void complete(LineReader reader, ParsedLine cur, List<Candidate> candidates) {
        TDL4ParsedLine pl = (TDL4ParsedLine) cur;

        if (pl.index == null) {
            return;
        }

        TDL4Parser parser = (TDL4Parser) reader.getParser();

        if (parser.curToken == null) {
            return;
        }

        int tokType = parser.tokens.get(parser.curToken).getType();
        if (COMPL_STMT.contains(tokType)) {
            completeStmt(tokType, candidates);
            return;
        }

        Integer stmtType = null;
        int stmtIndex = parser.curToken;
        for (; stmtIndex >= 0; stmtIndex--) {
            int iType = parser.tokens.get(stmtIndex).getType();
            if (iType == S_SCOL) {
                return;
            }

            if (COMPL_STMT.contains(iType)) {
                stmtType = iType;
                break;
            }
        }

        if (stmtType == null) {
            return;
        }

        int endIndex = parser.tokens.size();
        for (int i = parser.curToken; i < endIndex; i++) {
            if (parser.tokens.get(i).getType() == S_SCOL) {
                endIndex = i;
                break;
            }
        }

        List<Token> tokens = parser.tokens.subList(stmtIndex, endIndex);

        int tokPos = pl.index - stmtIndex;
        switch (stmtType) {
            case K_CREATE: {
                break;
            }
            case K_TRANSFORM: {
                switch (tokType) {
                    case K_DS: {
                        data.getAll().forEach(s -> candidates.add(new Candidate("DS " + escapeId(s))));

                        break;
                    }
                    case L_IDENTIFIER: {
                        int prevTok = tokens.get(tokPos - 1).getType();

                        switch (prevTok) {
                            case K_DS:
                            case K_TRANSFORM: {
                                vars.getAll().forEach(s -> candidates.add(new Candidate(escapeId(s))));

                                break;
                            }
                            case L_IDENTIFIER: {
                                if (pl.index < 4) {
                                    Transforms.TRANSFORMS.keySet().forEach(s -> candidates.add(new Candidate(escapeId(s) + "()")));
                                }

                                break;
                            }
                            case S_AT: {
                                for (int i = 3; i > 0; i--) {
                                    if (tokens.get(i).getType() == L_IDENTIFIER) {
                                        var v = Transforms.TRANSFORMS.get(tokens.get(i).getText());
                                        if (v != null) {
                                            v.meta.definitions.keySet().forEach(s -> candidates.add(new Candidate(escapeId(s) + " =")));

                                            break;
                                        }
                                    }
                                }

                                break;
                            }
                            case S_EQ:
                            case K_KEY: {
                                DataStream ds = dsFromTokens(tokens);
                                if (ds != null) {
                                    ds.accessor.attributes(OBJLVL_VALUE).forEach(this::escapeId);
                                }

                                break;
                            }
                        }

                        break;
                    }
                    case S_AT: {
                        for (int i = 3; i > 0; i--) {
                            if (tokens.get(i).getType() == L_IDENTIFIER) {
                                var v = Transforms.TRANSFORMS.get(tokens.get(i).getText());
                                if (v != null) {
                                    v.meta.definitions.keySet().forEach(s -> candidates.add(new Candidate("@" + escapeId(s) + " =")));

                                    break;
                                }
                            }
                        }

                        break;
                    }
                }

                break;
            }
            case K_COPY: {
                break;
            }
            case K_LET: {
                if (tokType == S_DOLLAR) {
                    vars.getAll().forEach(s -> candidates.add(new Candidate("$" + escapeId(s))));

                    break;
                }
                if (tokType == L_IDENTIFIER) {
                    vars.getAll().forEach(s -> candidates.add(new Candidate(escapeId(s))));

                    break;
                }
                if ((tokType == S_EQ) && (tokPos == 3)) {
                    candidates.add(new Candidate("= ARRAY[];"));
                    candidates.add(new Candidate("= SELECT"));

                    break;
                }

                break;
            }
            case K_LOOP: {
                break;
            }
            case K_IF: {
                break;
            }
            case K_SELECT: {
                break;
            }
            case K_CALL: {
                break;
            }
            case K_ANALYZE: {
                if ((tokType == L_IDENTIFIER) && (tokPos < 3)) {
                    data.getAll().forEach(s -> candidates.add(new Candidate(escapeId(s))));

                    break;
                }
                if (tokType == K_DS) {
                    data.getAll().forEach(s -> candidates.add(new Candidate("DS " + escapeId(s))));

                    break;
                }
                if (tokType == K_KEY) {
                    DataStream ds = dsFromTokens(tokens.subList(stmtIndex, tokPos));
                    if (ds != null) {
                        ds.accessor.attributes(OBJLVL_VALUE).forEach(s -> candidates.add(new Candidate("KEY " + escapeId(s) + ";")));
                    }

                    break;
                }
                if ((tokType == L_IDENTIFIER) && (tokPos > 3)) {
                    DataStream ds = dsFromTokens(tokens.subList(stmtIndex, tokPos));
                    if (ds != null) {
                        ds.accessor.attributes(OBJLVL_VALUE).forEach(s -> candidates.add(new Candidate(escapeId(s) + ";")));
                    }

                    break;
                }

                break;
            }
            case K_OPTIONS: {
                break;
            }
        }
    }

    private void completeStmt(int tokType, List<Candidate> candidates) {
        switch (tokType) {
            case K_CREATE: {
                break;
            }
            case K_TRANSFORM: {
                data.getAll().forEach(s -> candidates.add(new Candidate("DS " + escapeId(s))));

                break;
            }
            case K_COPY: {
                break;
            }
            case K_LET: {
                vars.getAll().forEach(s -> candidates.add(new Candidate("LET $" + escapeId(s) + " =")));

                break;
            }
            case K_LOOP: {
                break;
            }
            case K_IF: {
                break;
            }
            case K_SELECT: {
                break;
            }
            case K_CALL: {
                break;
            }
            case K_ANALYZE: {
                data.getAll().forEach(s -> candidates.add(new Candidate("ANALYZE " + escapeId(s) + ";")));

                break;
            }
            case K_OPTIONS: {
                break;
            }
        }
    }

    private DataStream dsFromTokens(List<Token> tokens) {
        for (Token token : tokens) {
            if (token.getType() == L_IDENTIFIER) {
                String dsName = unescapeId(token.getText());

                if (!data.has(dsName)) {
                    return null;
                }

                return data.get(dsName);
            }
        }

        return null;
    }

    private String unescapeId(String s) {
        if ((s.charAt(0) == '"') && (s.charAt(s.length() - 1) == '"')) {
            s = s.substring(1, s.length() - 1);
        }
        s = s.replace("\"\"", "\"");

        return s;
    }

    private String escapeId(String s) {
        if (s.contains("\"")) {
            s = s.replaceAll("\"", "\"\"");
        }

        if (!ID_PATTERN.matcher(s).matches()) {
            s = "\"" + s + "\"";
        }

        return s;
    }
}
