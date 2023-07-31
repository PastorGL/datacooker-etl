/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.cli.repl;

import io.github.pastorgl.datacooker.Constants;
import io.github.pastorgl.datacooker.Options;
import io.github.pastorgl.datacooker.RegisteredPackages;
import io.github.pastorgl.datacooker.data.DataContext;
import io.github.pastorgl.datacooker.data.DataStream;
import io.github.pastorgl.datacooker.data.Transforms;
import io.github.pastorgl.datacooker.metadata.NamedStreamsMeta;
import io.github.pastorgl.datacooker.scripting.Operations;
import io.github.pastorgl.datacooker.scripting.VariablesContext;
import io.github.pastorgl.datacooker.storage.Adapters;
import org.antlr.v4.runtime.Token;
import org.jline.reader.Candidate;
import org.jline.reader.Completer;
import org.jline.reader.LineReader;
import org.jline.reader.ParsedLine;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static io.github.pastorgl.datacooker.Constants.OBJLVL_VALUE;
import static io.github.pastorgl.datacooker.scripting.TDL4.*;

public class ReplCompleter implements Completer {
    private final VariablesContext vars;
    private final DataContext data;

    private final Set<Integer> COMPL_STMT = Set.of(K_CREATE, K_TRANSFORM, K_COPY, K_LET, K_LOOP, K_IF, K_SELECT, K_CALL, K_ANALYZE, K_OPTIONS);
    private final Pattern ID_PATTERN = Pattern.compile("[a-z_][a-z_0-9.]*", Pattern.CASE_INSENSITIVE);

    private final Random random = new Random();

    public ReplCompleter(VariablesContext variablesContext, DataContext dataContext) {
        vars = variablesContext;
        data = dataContext;
    }

    @Override
    public void complete(LineReader reader, ParsedLine cur, List<Candidate> candidates) {
        if (cur instanceof ReplParsedLine) {
            ReplParsedLine rpl = (ReplParsedLine) cur;
            if (rpl.command) {
                completeCommand(reader, rpl, candidates);
            } else {
                completeTDL4(reader, rpl, candidates);
            }
        }
    }

    private void completeCommand(LineReader reader, ReplParsedLine rpl, List<Candidate> candidates) {
        if (rpl.index == 0) {
            Arrays.stream(Command.values()).map(Enum::name).forEach(s -> candidates.add(new Candidate("\\" + s)));

            return;
        }

        Command c = Command.get(rpl.words.get(0).trim().substring(1));
        if (c == null) {
            return;
        }

        switch (c) {
            case HELP: {
                Arrays.stream(Command.values()).map(Enum::name).forEach(s -> candidates.add(new Candidate("\\" + s + ";")));

                break;
            }
            case EVAL: {
                vars.getAll().forEach(s -> candidates.add(new Candidate("$" + escapeId(s))));

                break;
            }
            case PRINT: {
                data.getAll().forEach(s -> candidates.add(new Candidate(escapeId(s + ";"))));

                break;
            }
            case SHOW: {
                candidates.add(new Candidate("DS;"));
                candidates.add(new Candidate("VARIABLE;"));
                candidates.add(new Candidate("PACKAGE;"));
                candidates.add(new Candidate("TRANSFORM;"));
                candidates.add(new Candidate("OPERATION;"));
                candidates.add(new Candidate("INPUT;"));
                candidates.add(new Candidate("OUTPUT;"));
                candidates.add(new Candidate("OPTION;"));

                break;
            }
            case DESCRIBE: {
                String ent = rpl.words.get(1);

                describe:
                {
                    if (ent.startsWith("DS")) {
                        data.getAll().forEach(ds -> candidates.add(new Candidate("DS " + escapeId(ds + ";"))));
                        break describe;
                    }
                    if (ent.startsWith("VARIABLE")) {
                        vars.getAll().forEach(v -> candidates.add(new Candidate("VARIABLE " + escapeId(v + ";"))));
                        break describe;
                    }
                    if (ent.startsWith("PACKAGE")) {
                        RegisteredPackages.REGISTERED_PACKAGES.keySet().forEach(s -> candidates.add(new Candidate("PACKAGE " + s + ";")));
                        break describe;
                    }
                    if (ent.startsWith("TRANSFORM")) {
                        Transforms.TRANSFORMS.keySet().forEach(s -> candidates.add(new Candidate("TRANSFORM " + s + ";")));
                        break describe;
                    }
                    if (ent.startsWith("OPERATION")) {
                        Operations.OPERATIONS.keySet().forEach(s -> candidates.add(new Candidate("OPERATION " + s + ";")));
                        break describe;
                    }
                    if (ent.startsWith("INPUT")) {
                        Adapters.INPUTS.keySet().forEach(s -> candidates.add(new Candidate("INPUT " + s + ";")));
                        break describe;
                    }
                    if (ent.startsWith("OUTPUT")) {
                        Adapters.OUTPUTS.keySet().forEach(s -> candidates.add(new Candidate("OUTPUT " + s + ";")));
                        break describe;
                    }
                    if (ent.startsWith("OPTION")) {
                        Arrays.stream(Options.values()).map(Enum::name).forEach(s -> candidates.add(new Candidate("OPTION " + s + ";")));
                        break describe;
                    }

                    candidates.add(new Candidate("DS"));
                    candidates.add(new Candidate("VARIABLE"));
                    candidates.add(new Candidate("PACKAGE"));
                    candidates.add(new Candidate("TRANSFORM"));
                    candidates.add(new Candidate("OPERATION"));
                    candidates.add(new Candidate("INPUT"));
                    candidates.add(new Candidate("OUTPUT"));
                    candidates.add(new Candidate("OPTION"));
                }

                break;
            }
        }
    }

    private void completeTDL4(LineReader reader, ReplParsedLine rpl, List<Candidate> candidates) {
        if (rpl.index == null) {
            return;
        }

        ReplParser parser = (ReplParser) reader.getParser();

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

        List<Token> stmtToks = parser.tokens.subList(stmtIndex, endIndex);

        int tokPos = rpl.index - stmtIndex;
        switch (stmtType) {
            case K_CREATE: {
                switch (tokType) {
                    case K_CREATE: {
                        String dsName = escapeId("ds" + random.nextInt());
                        Adapters.INPUTS.keySet().forEach(s -> candidates.add(new Candidate("CREATE DS " + dsName + " " + escapeId(s) + "() FROM")));

                        break;
                    }
                    case K_PARTITION: {
                        vars.getAll().forEach(s -> candidates.add(new Candidate("PARTITION $" + escapeId(s) + " BY")));

                        break;
                    }
                    case K_BY: {
                        candidates.add(new Candidate("BY HASHCODE;"));
                        candidates.add(new Candidate("BY SOURCE;"));
                        candidates.add(new Candidate("BY RANDOM;"));

                        break;
                    }
                    case K_FROM: {
                        for (int i = 3; i > 0; i--) {
                            if (stmtToks.get(i).getType() == L_IDENTIFIER) {
                                var v = Adapters.INPUTS.get(unescapeId(stmtToks.get(i).getText()));
                                if (v != null) {
                                    Arrays.stream(v.meta.paths).forEach(s -> candidates.add(new Candidate("FROM '" + s + "'")));

                                    break;
                                }
                            }
                        }
                        vars.getAll().forEach(s -> candidates.add(new Candidate("FROM $" + escapeId(s))));

                        break;
                    }
                    case L_IDENTIFIER: {
                        int prevTok = stmtToks.get(tokPos - 1).getType();

                        switch (prevTok) {
                            case S_AT: {
                                for (int i = 3; i > 0; i--) {
                                    if (stmtToks.get(i).getType() == L_IDENTIFIER) {
                                        var v = Adapters.INPUTS.get(unescapeId(stmtToks.get(i).getText()));
                                        if (v != null) {
                                            v.meta.definitions.keySet().forEach(s -> candidates.add(new Candidate(escapeId(s) + " =")));

                                            break;
                                        }
                                    }
                                }

                                break;
                            }
                            case S_DOLLAR: {
                                vars.getAll().forEach(s -> candidates.add(new Candidate(escapeId(s))));

                                break;
                            }
                            case L_IDENTIFIER: {
                                if (tokPos < 4) {
                                    Adapters.INPUTS.keySet().forEach(s -> candidates.add(new Candidate(escapeId(s) + "()")));
                                }

                                break;
                            }
                        }

                        break;
                    }
                    case S_AT: {
                        for (int i = 3; i > 0; i--) {
                            if (stmtToks.get(i).getType() == L_IDENTIFIER) {
                                var v = Adapters.INPUTS.get(unescapeId(stmtToks.get(i).getText()));
                                if (v != null) {
                                    v.meta.definitions.keySet().forEach(s -> candidates.add(new Candidate("@" + escapeId(s) + " =")));

                                    break;
                                }
                            }
                        }

                        break;
                    }
                    case S_OPEN_PAR: {
                        var v = Adapters.INPUTS.get(unescapeId(stmtToks.get(tokPos - 1).getText()));
                        if (v != null) {
                            candidates.add(new Candidate("(" + v.meta.definitions.keySet().stream().map(s -> "@" + escapeId(s) + " = ").collect(Collectors.joining(","))));
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
                        DataStream ds = dsFromTokens(stmtToks);
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
                        DataStream ds = dsFromTokens(stmtToks);
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
                            if (stmtToks.get(i).getType() == L_IDENTIFIER) {
                                var v = Transforms.TRANSFORMS.get(unescapeId(stmtToks.get(i).getText()));
                                if (v != null) {
                                    v.meta.definitions.keySet().forEach(s -> candidates.add(new Candidate("@" + escapeId(s) + " =")));

                                    break;
                                }
                            }
                        }

                        break;
                    }
                    case S_EQ: {
                        DataStream ds = dsFromTokens(stmtToks);
                        if (ds != null) {
                            ds.accessor.attributes(OBJLVL_VALUE).forEach(s -> candidates.add(new Candidate("= " + escapeId(s))));
                        }
                        vars.getAll().forEach(s -> candidates.add(new Candidate("= $" + escapeId(s))));

                        break;
                    }
                    case S_OPEN_PAR: {
                        int prevTok = stmtToks.get(tokPos - 1).getType();

                        switch (prevTok) {
                            case K_COLUMNS: {
                                DataStream ds = dsFromTokens(stmtToks);
                                if (ds != null) {
                                    String objLvl = stmtToks.get(tokPos - 2).getText();

                                    candidates.add(new Candidate("(" + String.join(", ", ds.accessor.attributes(objLvl))));
                                }

                                break;
                            }
                            case T_POINT:
                            case T_POLYGON:
                            case T_SEGMENT:
                            case T_TRACK:
                            case T_VALUE: {
                                DataStream ds = dsFromTokens(stmtToks);
                                if (ds != null) {
                                    String objLvl = stmtToks.get(tokPos - 1).getText();

                                    candidates.add(new Candidate("(" + String.join(", ", ds.accessor.attributes(objLvl))));
                                }

                                break;
                            }
                            case L_IDENTIFIER: {
                                var v = Transforms.TRANSFORMS.get(unescapeId(stmtToks.get(tokPos - 1).getText()));
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
                        int prevTok = stmtToks.get(tokPos - 1).getType();

                        switch (prevTok) {
                            case K_DS:
                            case K_TRANSFORM: {
                                data.getAll().forEach(s -> candidates.add(new Candidate(escapeId(s))));

                                break;
                            }
                            case S_AT: {
                                for (int i = 3; i > 0; i--) {
                                    if (stmtToks.get(i).getType() == L_IDENTIFIER) {
                                        var v = Transforms.TRANSFORMS.get(unescapeId(stmtToks.get(i).getText()));
                                        if (v != null) {
                                            v.meta.definitions.keySet().forEach(s -> candidates.add(new Candidate(escapeId(s) + " =")));

                                            break;
                                        }
                                    }
                                }

                                break;
                            }
                            case S_EQ: {
                                DataStream ds = dsFromTokens(stmtToks);
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
                                DataStream ds = dsFromTokens(stmtToks);
                                if (ds != null) {
                                    ds.accessor.attributes(OBJLVL_VALUE).forEach(s -> candidates.add(new Candidate(escapeId(s))));
                                }

                                break;
                            }
                            case S_STAR: {
                                if (tokPos < 5) {
                                    Transforms.TRANSFORMS.keySet().forEach(s -> candidates.add(new Candidate(escapeId(s) + "()")));
                                }

                                break;
                            }
                            case L_IDENTIFIER: {
                                if (tokPos < 4) {
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
                        data.getAll().forEach(s -> candidates.add(new Candidate("COPY DS " + escapeId(s))));

                        break;
                    }
                    case K_DS: {
                        data.getAll().forEach(s -> candidates.add(new Candidate("DS " + escapeId(s))));

                        break;
                    }
                    case K_INTO: {
                        for (int i = 3; i > 0; i--) {
                            if (stmtToks.get(i).getType() == L_IDENTIFIER) {
                                var v = Adapters.OUTPUTS.get(unescapeId(stmtToks.get(i).getText()));
                                if (v != null) {
                                    Arrays.stream(v.meta.paths).forEach(s -> candidates.add(new Candidate("INTO '" + s + "'")));

                                    break;
                                }
                            }
                        }
                        vars.getAll().forEach(s -> candidates.add(new Candidate("INTO $" + escapeId(s))));

                        break;
                    }
                    case L_IDENTIFIER: {
                        int prevTok = stmtToks.get(tokPos - 1).getType();

                        switch (prevTok) {
                            case K_DS:
                            case K_COPY: {
                                data.getAll().forEach(s -> candidates.add(new Candidate(escapeId(s))));

                                break;
                            }
                            case S_AT: {
                                for (int i = 3; i > 0; i--) {
                                    if (stmtToks.get(i).getType() == L_IDENTIFIER) {
                                        var v = Adapters.OUTPUTS.get(unescapeId(stmtToks.get(i).getText()));
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
                                if (tokPos < 5) {
                                    Adapters.OUTPUTS.keySet().forEach(s -> candidates.add(new Candidate(escapeId(s) + "()")));
                                }

                                break;
                            }
                            case L_IDENTIFIER: {
                                if (tokPos < 4) {
                                    candidates.add(new Candidate(Constants.STAR));
                                    Adapters.OUTPUTS.keySet().forEach(s -> candidates.add(new Candidate(escapeId(s) + "()")));
                                }

                                break;
                            }
                        }

                        break;
                    }
                    case S_AT: {
                        for (int i = 3; i > 0; i--) {
                            if (stmtToks.get(i).getType() == L_IDENTIFIER) {
                                var v = Adapters.OUTPUTS.get(unescapeId(stmtToks.get(i).getText()));
                                if (v != null) {
                                    v.meta.definitions.keySet().forEach(s -> candidates.add(new Candidate("@" + escapeId(s) + " =")));

                                    break;
                                }
                            }
                        }

                        break;
                    }
                    case S_OPEN_PAR: {
                        var v = Adapters.OUTPUTS.get(unescapeId(stmtToks.get(tokPos - 1).getText()));
                        if (v != null) {
                            candidates.add(new Candidate("(" + v.meta.definitions.keySet().stream().map(s -> "@" + escapeId(s) + " = ").collect(Collectors.joining(","))));
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
                    case S_DOLLAR: {
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
                    case K_IF: {
                        vars.getAll().forEach(s -> candidates.add(new Candidate("IF $" + escapeId(s))));

                        break;
                    }
                    case S_DOLLAR: {
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
                switch (tokType) {
                    case K_SELECT: {
                        candidates.add(new Candidate("SELECT * FROM"));
                        candidates.add(new Candidate("SELECT DISTINCT * FROM"));

                        break;
                    }
                    case K_FROM: {
                        candidates.add(new Candidate("FROM JOIN"));
                        candidates.add(new Candidate("FROM LEFT JOIN"));
                        candidates.add(new Candidate("FROM RIGHT JOIN"));
                        candidates.add(new Candidate("FROM LEFT ANTI JOIN"));
                        candidates.add(new Candidate("FROM RIGHT ANTI JOIN"));
                        candidates.add(new Candidate("FROM OUTER JOIN"));
                        candidates.add(new Candidate("FROM UNION"));
                        candidates.add(new Candidate("FROM UNION XOR"));
                        candidates.add(new Candidate("FROM UNION AND"));
                        data.getAll().forEach(ds -> candidates.add(new Candidate("FROM " + escapeId(ds))));

                        break;
                    }
                    case K_JOIN: {
                        data.getAll().forEach(ds -> candidates.add(new Candidate("JOIN " + escapeId(ds) + ",")));

                        break;
                    }
                    case K_UNION: {
                        data.getAll().forEach(ds -> candidates.add(new Candidate("UNION " + escapeId(ds) + ",")));

                        break;
                    }
                }

                break;
            }
            case K_CALL: {
                switch (tokType) {
                    case K_CALL: {
                        Operations.OPERATIONS.keySet().forEach(s -> candidates.add(new Candidate("CALL " + escapeId(s) + "()")));

                        break;
                    }
                    case K_INPUT: {
                        for (int i = 2; i > 0; i--) {
                            if (stmtToks.get(i).getType() == L_IDENTIFIER) {
                                var v = Operations.OPERATIONS.get(unescapeId(stmtToks.get(i).getText()));
                                if (v != null) {
                                    var inp = v.meta.input;

                                    final StringBuilder can = new StringBuilder("INPUT ");
                                    if (inp instanceof NamedStreamsMeta) {
                                        can.append(((NamedStreamsMeta) inp).streams.keySet().stream().map(s -> s + " FROM ").collect(Collectors.joining(",")));
                                    }

                                    candidates.add(new Candidate(can.toString()));

                                    break;
                                }
                            }
                        }

                        break;
                    }
                    case K_OUTPUT: {
                        for (int i = 2; i > 0; i--) {
                            if (stmtToks.get(i).getType() == L_IDENTIFIER) {
                                var v = Operations.OPERATIONS.get(unescapeId(stmtToks.get(i).getText()));
                                if (v != null) {
                                    var out = v.meta.output;

                                    final StringBuilder can = new StringBuilder("OUTPUT ");
                                    if (out instanceof NamedStreamsMeta) {
                                        can.append(((NamedStreamsMeta) out).streams.keySet().stream().map(s -> s + " INTO ").collect(Collectors.joining(",")));
                                    }

                                    candidates.add(new Candidate(can.toString()));

                                    break;
                                }
                            }
                        }

                        break;
                    }
                    case S_AT: {
                        for (int i = 2; i > 0; i--) {
                            if (stmtToks.get(i).getType() == L_IDENTIFIER) {
                                var v = Operations.OPERATIONS.get(unescapeId(stmtToks.get(i).getText()));
                                if (v != null) {
                                    v.meta.definitions.keySet().forEach(s -> candidates.add(new Candidate("@" + escapeId(s) + " =")));

                                    break;
                                }
                            }
                        }

                        break;
                    }
                    case S_EQ: {
/*                        DataStream ds = dsFromTokens(stmtToks);
                        if (ds != null) {
                            ds.accessor.attributes(OBJLVL_VALUE).forEach(s -> candidates.add(new Candidate("= " + escapeId(s))));
                        }*/
                        vars.getAll().forEach(s -> candidates.add(new Candidate("= $" + escapeId(s))));

                        break;
                    }
                    case S_OPEN_PAR: {
                        var v = Operations.OPERATIONS.get(unescapeId(stmtToks.get(tokPos - 1).getText()));
                        if (v != null) {
                            candidates.add(new Candidate("(" + v.meta.definitions.keySet().stream().map(s -> "@" + escapeId(s) + " = ").collect(Collectors.joining(","))));
                        }

                        break;
                    }
                    case S_CLOSE_PAR: {
                        for (int i = 2; i > 0; i--) {
                            if (stmtToks.get(i).getType() == L_IDENTIFIER) {
                                var v = Operations.OPERATIONS.get(unescapeId(stmtToks.get(i).getText()));
                                if (v != null) {
                                    var inp = v.meta.input;
                                    var out = v.meta.output;

                                    final StringBuilder can = new StringBuilder("INPUT ");
                                    if (inp instanceof NamedStreamsMeta) {
                                        can.append(((NamedStreamsMeta) inp).streams.keySet().stream().map(s -> s + " FROM ").collect(Collectors.joining(",")));
                                    }
                                    can.append("OUTPUT ");
                                    if (out instanceof NamedStreamsMeta) {
                                        can.append(((NamedStreamsMeta) out).streams.keySet().stream().map(s -> s + " INTO ").collect(Collectors.joining(",")));
                                    }

                                    candidates.add(new Candidate(can.toString()));

                                    break;
                                }
                            }
                        }

                        break;
                    }
                    case L_IDENTIFIER: {
                        int prevTok = stmtToks.get(tokPos - 1).getType();

                        switch (prevTok) {
                            case S_AT: {
                                for (int i = 2; i > 0; i--) {
                                    if (stmtToks.get(i).getType() == L_IDENTIFIER) {
                                        var v = Operations.OPERATIONS.get(unescapeId(stmtToks.get(i).getText()));
                                        if (v != null) {
                                            v.meta.definitions.keySet().forEach(s -> candidates.add(new Candidate(escapeId(s) + " =")));

                                            break;
                                        }
                                    }
                                }

                                break;
                            }
                            case S_EQ: {
/*                                DataStream ds = dsFromTokens(stmtToks);
                                if (ds != null) {
                                    ds.accessor.attributes(OBJLVL_VALUE).forEach(s -> candidates.add(new Candidate(escapeId(s))));
                                }*/
                                vars.getAll().forEach(s -> candidates.add(new Candidate("$" + escapeId(s))));

                                break;
                            }
                            case S_DOLLAR: {
                                vars.getAll().forEach(s -> candidates.add(new Candidate(escapeId(s))));

                                break;
                            }
                            case L_IDENTIFIER: {
                                if (tokPos < 3) {
                                    candidates.add(new Candidate(Constants.STAR));
                                    Operations.OPERATIONS.keySet().forEach(s -> candidates.add(new Candidate(escapeId(s) + "()")));
                                }

                                break;
                            }
                        }

                        break;
                    }
                }

                break;
            }
            case K_ANALYZE: {
                switch (tokType) {
                    case K_ANALYZE: {
                        data.getAll().forEach(s -> candidates.add(new Candidate("ANALYZE DS " + escapeId(s))));

                        break;
                    }
                    case K_DS: {
                        data.getAll().forEach(s -> candidates.add(new Candidate("DS " + escapeId(s))));

                        break;
                    }
                    case K_KEY: {
                        DataStream ds = dsFromTokens(stmtToks.subList(stmtIndex, tokPos));
                        if (ds != null) {
                            ds.accessor.attributes(OBJLVL_VALUE).forEach(s -> candidates.add(new Candidate("KEY " + escapeId(s) + ";")));
                        }

                        break;
                    }
                    case L_IDENTIFIER: {
                        if (tokPos < 3) {
                            data.getAll().forEach(s -> candidates.add(new Candidate(escapeId(s))));
                        } else {
                            DataStream ds = dsFromTokens(stmtToks.subList(stmtIndex, tokPos));
                            if (ds != null) {
                                ds.accessor.attributes(OBJLVL_VALUE).forEach(s -> candidates.add(new Candidate(escapeId(s))));
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
                        Arrays.stream(Options.values()).forEach(o -> candidates.add(new Candidate("OPTIONS @" + escapeId(o.name()) + " =")));

                        break;
                    }
                    case S_AT: {
                        Arrays.stream(Options.values()).forEach(o -> candidates.add(new Candidate("@" + escapeId(o.name()) + " =")));

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
                    case S_DOLLAR: {
                        vars.getAll().forEach(s -> candidates.add(new Candidate("$" + escapeId(s))));

                        break;
                    }
                    case S_EQ: {
                        vars.getAll().forEach(s -> candidates.add(new Candidate("= $" + escapeId(s))));

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
