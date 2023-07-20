/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.cli;

import io.github.pastorgl.datacooker.Constants;
import io.github.pastorgl.datacooker.Options;
import io.github.pastorgl.datacooker.data.DataContext;
import io.github.pastorgl.datacooker.data.DataStream;
import io.github.pastorgl.datacooker.data.Transforms;
import io.github.pastorgl.datacooker.scripting.VariablesContext;
import io.github.pastorgl.datacooker.storage.Adapters;
import org.antlr.v4.runtime.Token;
import org.apache.hadoop.shaded.org.checkerframework.checker.units.qual.C;
import org.jline.reader.Candidate;
import org.jline.reader.Completer;
import org.jline.reader.LineReader;
import org.jline.reader.ParsedLine;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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
                switch (tokType) {
                    case K_CREATE: {
                        candidates.add(new Candidate("CREATE DS"));

                        break;
                    }
                    case K_PARTITION: {
                        vars.getAll().forEach(s -> candidates.add(new Candidate("PARTITION $" + escapeId(s))));

                        break;
                    }
                    case K_BY: {
                        candidates.add(new Candidate("BY HASHCODE"));
                        candidates.add(new Candidate("BY SOURCE"));
                        candidates.add(new Candidate("BY RANDOM"));

                        break;
                    }
                    case K_FROM: {
                        vars.getAll().forEach(s -> candidates.add(new Candidate("FROM $" + escapeId(s))));

                        break;
                    }
                    case L_IDENTIFIER: {
                        int prevTok = tokens.get(tokPos - 1).getType();

                        switch (prevTok) {
                            case S_AT: {
                                for (int i = 3; i > 0; i--) {
                                    if (tokens.get(i).getType() == L_IDENTIFIER) {
                                        var v = Adapters.INPUTS.get(unescapeId(tokens.get(i).getText()));
                                        if (v != null) {
                                            v.meta.definitions.keySet().forEach(s -> candidates.add(new Candidate(escapeId(s) + " =")));

                                            break;
                                        }
                                    }
                                }

                                break;
                            }
                            case S_EQ: {
                                vars.getAll().forEach(s -> candidates.add(new Candidate("$" + escapeId(s))));

                                break;
                            }
                            case S_DOLLAR: {
                                vars.getAll().forEach(s -> candidates.add(new Candidate(escapeId(s))));

                                break;
                            }
                            case L_IDENTIFIER: {
                                if (pl.index < 4) {
                                    Adapters.INPUTS.keySet().forEach(s -> candidates.add(new Candidate(escapeId(s) + "()")));
                                }

                                break;
                            }
                        }

                        break;
                    }
                }
                break;
            }
            case K_TRANSFORM: {
                switch (tokType) {
                    case K_TRANSFORM: {
                        data.getAll().forEach(s -> candidates.add(new Candidate("TRANSFORM " + escapeId(s))));

                        break;
                    }
                    case K_DS: {
                        data.getAll().forEach(s -> candidates.add(new Candidate("DS " + escapeId(s))));

                        break;
                    }
                    case K_KEY: {
                        DataStream ds = dsFromTokens(tokens);
                        if (ds != null) {
                            ds.accessor.attributes(OBJLVL_VALUE).forEach(s -> candidates.add(new Candidate("KEY " + escapeId(s))));
                        }

                        break;
                    }
                    case K_PARTITION: {
                        vars.getAll().forEach(s -> candidates.add(new Candidate("PARTITION $" + escapeId(s))));

                        break;
                    }
                    case K_SET: {
                        DataStream ds = dsFromTokens(tokens);
                        if (ds != null) {
                            switch (ds.streamType) {
                                case Point: {
                                    candidates.add(new Candidate("SET Point COLUMNS()"));
                                    break;
                                }
                                case Track: {
                                    candidates.add(new Candidate("SET Track COLUMNS()"));
                                    candidates.add(new Candidate("SET Segment COLUMNS()"));
                                    candidates.add(new Candidate("SET Point COLUMNS()"));
                                    break;
                                }
                                case Polygon: {
                                    candidates.add(new Candidate("SET Polygon COLUMNS()"));
                                    break;
                                }
                                case Structured:
                                case Columnar: {
                                    candidates.add(new Candidate("SET Value COLUMNS()"));
                                    break;
                                }
                            }
                        }

                        break;
                    }
                    case S_AT: {
                        for (int i = 3; i > 0; i--) {
                            if (tokens.get(i).getType() == L_IDENTIFIER) {
                                var v = Transforms.TRANSFORMS.get(unescapeId(tokens.get(i).getText()));
                                if (v != null) {
                                    v.meta.definitions.keySet().forEach(s -> candidates.add(new Candidate("@" + escapeId(s) + " =")));

                                    break;
                                }
                            }
                        }

                        break;
                    }
                    case S_EQ: {
                        DataStream ds = dsFromTokens(tokens);
                        if (ds != null) {
                            ds.accessor.attributes(OBJLVL_VALUE).forEach(s -> candidates.add(new Candidate("= " + escapeId(s))));
                        }
                        vars.getAll().forEach(s -> candidates.add(new Candidate("= $" + escapeId(s))));

                        break;
                    }
                    case S_OPEN_PAR: {
                        int prevTok = tokens.get(tokPos - 1).getType();

                        switch (prevTok) {
                            case K_COLUMNS: {
                                DataStream ds = dsFromTokens(tokens);
                                if (ds != null) {
                                    String objLvl = tokens.get(tokPos - 2).getText();

                                    candidates.add(new Candidate("(" + String.join(", ", ds.accessor.attributes(objLvl))));
                                }

                                break;
                            }
                            case T_POINT:
                            case T_POLYGON:
                            case T_SEGMENT:
                            case T_TRACK:
                            case T_VALUE: {
                                DataStream ds = dsFromTokens(tokens);
                                if (ds != null) {
                                    String objLvl = tokens.get(tokPos - 1).getText();

                                    candidates.add(new Candidate("(" + String.join(", ", ds.accessor.attributes(objLvl))));
                                }

                                break;
                            }
                            case L_IDENTIFIER: {
                                var v = Transforms.TRANSFORMS.get(unescapeId(tokens.get(tokPos - 1).getText()));
                                if (v != null) {
                                    candidates.add(new Candidate("(" + v.meta.definitions.keySet().stream().map(s -> "@" + escapeId(s) + " = ").collect(Collectors.joining(","))));

                                    break;
                                }
                                break;
                            }
                        }

                        break;
                    }
                    case L_IDENTIFIER: {
                        int prevTok = tokens.get(tokPos - 1).getType();

                        switch (prevTok) {
                            case K_DS:
                            case K_TRANSFORM: {
                                data.getAll().forEach(s -> candidates.add(new Candidate(escapeId(s))));

                                break;
                            }
                            case S_AT: {
                                for (int i = 3; i > 0; i--) {
                                    if (tokens.get(i).getType() == L_IDENTIFIER) {
                                        var v = Transforms.TRANSFORMS.get(unescapeId(tokens.get(i).getText()));
                                        if (v != null) {
                                            v.meta.definitions.keySet().forEach(s -> candidates.add(new Candidate(escapeId(s) + " =")));

                                            break;
                                        }
                                    }
                                }

                                break;
                            }
                            case S_EQ: {
                                DataStream ds = dsFromTokens(tokens);
                                if (ds != null) {
                                    ds.accessor.attributes(OBJLVL_VALUE).forEach(s -> candidates.add(new Candidate(escapeId(s))));
                                }
                                vars.getAll().forEach(s -> candidates.add(new Candidate("$" + escapeId(s))));

                                break;
                            }
                            case S_DOLLAR: {
                                vars.getAll().forEach(s -> candidates.add(new Candidate(escapeId(s))));

                                break;
                            }
                            case K_KEY: {
                                DataStream ds = dsFromTokens(tokens);
                                if (ds != null) {
                                    ds.accessor.attributes(OBJLVL_VALUE).forEach(s -> candidates.add(new Candidate(escapeId(s))));
                                }

                                break;
                            }
                            case S_STAR: {
                                if (pl.index < 5) {
                                    Transforms.TRANSFORMS.keySet().forEach(s -> candidates.add(new Candidate(escapeId(s) + "()")));
                                }

                                break;
                            }
                            case L_IDENTIFIER: {
                                if (pl.index < 4) {
                                    candidates.add(new Candidate(Constants.STAR));
                                    Transforms.TRANSFORMS.keySet().forEach(s -> candidates.add(new Candidate(escapeId(s) + "()")));
                                }

                                break;
                            }
                        }

                        break;
                    }
                }

                break;
            }
            case K_COPY: {
                switch (tokType) {
                    case K_COPY: {
                        data.getAll().forEach(s -> candidates.add(new Candidate("COPY " + escapeId(s))));

                        break;
                    }
                    case K_DS: {
                        data.getAll().forEach(s -> candidates.add(new Candidate("DS " + escapeId(s))));

                        break;
                    }
                    case K_INTO: {
                        vars.getAll().forEach(s -> candidates.add(new Candidate("INTO $" + escapeId(s))));

                        break;
                    }
                    case L_IDENTIFIER: {
                        int prevTok = tokens.get(tokPos - 1).getType();

                        switch (prevTok) {
                            case K_DS:
                            case K_COPY: {
                                data.getAll().forEach(s -> candidates.add(new Candidate(escapeId(s))));

                                break;
                            }
                            case S_AT: {
                                for (int i = 3; i > 0; i--) {
                                    if (tokens.get(i).getType() == L_IDENTIFIER) {
                                        var v = Adapters.OUTPUTS.get(unescapeId(tokens.get(i).getText()));
                                        if (v != null) {
                                            v.meta.definitions.keySet().forEach(s -> candidates.add(new Candidate(escapeId(s) + " =")));

                                            break;
                                        }
                                    }
                                }

                                break;
                            }
                            case S_EQ: {
                                vars.getAll().forEach(s -> candidates.add(new Candidate("$" + escapeId(s))));

                                break;
                            }
                            case S_DOLLAR: {
                                vars.getAll().forEach(s -> candidates.add(new Candidate(escapeId(s))));

                                break;
                            }
                            case S_STAR: {
                                if (pl.index < 5) {
                                    Adapters.OUTPUTS.keySet().forEach(s -> candidates.add(new Candidate(escapeId(s) + "()")));
                                }

                                break;
                            }
                            case L_IDENTIFIER: {
                                if (pl.index < 4) {
                                    candidates.add(new Candidate(Constants.STAR));
                                    Adapters.OUTPUTS.keySet().forEach(s -> candidates.add(new Candidate(escapeId(s) + "()")));
                                }

                                break;
                            }
                        }

                        break;
                    }
                }

                break;
            }
            case K_LET: {
                switch (tokType) {
                    case K_LET: {
                        vars.getAll().forEach(s -> candidates.add(new Candidate("LET $" + escapeId(s) + " =")));
                        break;
                    }
                    case S_DOLLAR: {
                        vars.getAll().forEach(s -> candidates.add(new Candidate("$" + escapeId(s))));

                        break;
                    }
                    case S_EQ: {
                        if (tokPos <= 3) {
                            candidates.add(new Candidate("= ARRAY[];"));
                            candidates.add(new Candidate("= SELECT"));
                        }
                        break;
                    }
                    case L_IDENTIFIER: {
                        vars.getAll().forEach(s -> candidates.add(new Candidate(escapeId(s))));

                        break;
                    }
                }

                break;
            }
            case K_LOOP: {
                switch (tokType) {
                    case K_LOOP: {
                        candidates.add(new Candidate("LOOP $I IN"));

                        break;
                    }
                    case S_IN: {
                        candidates.add(new Candidate("IN ARRAY[] BEGIN"));
                        vars.getAll().forEach(s -> candidates.add(new Candidate("IN $" + escapeId(s) + " BEGIN")));

                        break;
                    }
                    case S_DOLLAR : {
                        if (tokPos > 3) {
                            vars.getAll().forEach(s -> candidates.add(new Candidate("$" + escapeId(s))));
                        }

                        break;
                    }
                    case L_IDENTIFIER: {
                        if (tokPos > 3) {
                            vars.getAll().forEach(s -> candidates.add(new Candidate(escapeId(s))));
                        }

                        break;
                    }
                }

                break;
            }
            case K_IF: {
                switch (tokType) {
                    case S_DOLLAR : {
                        vars.getAll().forEach(s -> candidates.add(new Candidate("$" + escapeId(s))));

                        break;
                    }
                    case L_IDENTIFIER: {
                        vars.getAll().forEach(s -> candidates.add(new Candidate(escapeId(s))));

                        break;
                    }
                }

                break;
            }
            case K_SELECT: {
                break;
            }
            case K_CALL: {
                break;
            }
            case K_ANALYZE: {
                switch (tokType) {
                    case K_ANALYZE: {
                        data.getAll().forEach(s -> candidates.add(new Candidate("ANALYZE " + escapeId(s) + ";")));

                        break;
                    }
                    case K_DS: {
                        data.getAll().forEach(s -> candidates.add(new Candidate("DS " + escapeId(s))));

                        break;
                    }
                    case K_KEY: {
                        DataStream ds = dsFromTokens(tokens.subList(stmtIndex, tokPos));
                        if (ds != null) {
                            ds.accessor.attributes(OBJLVL_VALUE).forEach(s -> candidates.add(new Candidate("KEY " + escapeId(s) + ";")));
                        }

                        break;
                    }
                    case L_IDENTIFIER: {
                        if (tokPos < 3) {
                            data.getAll().forEach(s -> candidates.add(new Candidate(escapeId(s))));
                        } else {
                            DataStream ds = dsFromTokens(tokens.subList(stmtIndex, tokPos));
                            if (ds != null) {
                                ds.accessor.attributes(OBJLVL_VALUE).forEach(s -> candidates.add(new Candidate(escapeId(s) + ";")));
                            }
                        }
                        break;
                    }
                }
                break;
            }
            case K_OPTIONS: {
                switch (tokType) {
                    case K_OPTIONS: {
                        Arrays.stream(Options.values()).forEach(o -> candidates.add(new Candidate("OPTIONS @" + escapeId(o.name()) + "=")));

                        break;
                    }
                    case S_AT: {
                        Arrays.stream(Options.values()).forEach(o -> candidates.add(new Candidate("@" + escapeId(o.name()) + "=")));

                        break;
                    }
                    case L_IDENTIFIER: {
                        if (tokPos < 3) {
                            Arrays.stream(Options.values()).forEach(o -> candidates.add(new Candidate(escapeId(o.name()))));
                        } else {
                            vars.getAll().forEach(s -> candidates.add(new Candidate(escapeId(s))));
                        }

                        break;
                    }
                    case S_DOLLAR : {
                        vars.getAll().forEach(s -> candidates.add(new Candidate("$" + escapeId(s))));

                        break;
                    }
                    case S_EQ: {
                        vars.getAll().forEach(s -> candidates.add(new Candidate("=$" + escapeId(s))));

                        break;
                    }
                }
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

    public static String unescapeId(String s) {
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
