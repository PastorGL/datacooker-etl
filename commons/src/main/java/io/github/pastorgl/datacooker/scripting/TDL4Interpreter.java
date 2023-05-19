/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.scripting;

import io.github.pastorgl.datacooker.Constants;
import io.github.pastorgl.datacooker.config.Configuration;
import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.data.*;
import io.github.pastorgl.datacooker.metadata.*;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.commons.collections4.map.ListOrderedMap;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.github.pastorgl.datacooker.Constants.*;
import static io.github.pastorgl.datacooker.data.DataContext.METRICS_COLUMNS;

public class TDL4Interpreter implements Iterable<TDL4.StatementContext> {
    private DataContext dataContext;
    private final TDL4.ScriptContext scriptContext;

    private final VariablesContext options;
    private VariablesContext variables = new VariablesContext();

    private static Number parseNumber(String sqlNumeric) {
        sqlNumeric = sqlNumeric.toLowerCase();
        if (sqlNumeric.endsWith("l")) {
            return Long.parseLong(sqlNumeric.substring(0, sqlNumeric.length() - 1));
        }
        if (sqlNumeric.startsWith("0x")) {
            return Long.parseUnsignedLong(sqlNumeric.substring(2), 16);
        }
        if (sqlNumeric.contains(".") || sqlNumeric.contains("e") || sqlNumeric.endsWith("d")) {
            return Double.parseDouble(sqlNumeric);
        }
        return Integer.parseInt(sqlNumeric);
    }

    private String parseString(String sqlString) {
        if (sqlString == null) {
            return null;
        }
        String string = sqlString;
        // SQL quoting character : '
        if ((string.charAt(0) == '\'') && (string.charAt(string.length() - 1) == '\'')) {
            string = string.substring(1, string.length() - 1);
        }
        string = string.replace("''", "'");

        return interpretString(string);
    }

    private String parseIdentifier(String sqlId) {
        if (sqlId == null) {
            return null;
        }
        String string = sqlId;
        // SQL quoting character : '
        if ((string.charAt(0) == '"') && (string.charAt(string.length() - 1) == '"')) {
            string = string.substring(1, string.length() - 1);
        }
        string = string.replace("\"\"", "\"");

        return interpretString(string);
    }

    private String interpretString(String interp) {
        int opBr = interp.indexOf('{');
        if (opBr >= 0) {
            if (interp.charAt(opBr - 1) != '\\') {
                int clBr = interp.indexOf('}', opBr);

                while (clBr >= 0) {
                    if (interp.charAt(clBr - 1) != '\\') {
                        interp = interp.substring(0, opBr) + interpretExpr(interp.substring(opBr + 1, clBr)) + interpretString(interp.substring(clBr + 1));
                        break;
                    } else {
                        clBr = interp.indexOf('}', clBr);
                    }
                }
            } else {
                interp = interp.substring(0, opBr + 1) + interpretString(interp.substring(opBr + 1));
            }
        }

        return interp.replace("\\{", "{").replace("\\}", "}");
    }

    private Object interpretExpr(String exprString) {
        if (exprString.isEmpty()) {
            return "";
        }

        CharStream cs = CharStreams.fromString(exprString);
        TDL4Lexicon lexer = new TDL4Lexicon(cs);
        TDL4 parser = new TDL4(new CommonTokenStream(lexer));

        TDL4ErrorListener errorListener = new TDL4ErrorListener();
        parser.addErrorListener(errorListener);

        TDL4.Loose_expressionContext exprContext = parser.loose_expression();

        if (errorListener.errorCount > 0) {
            List<String> errors = new ArrayList<>();
            for (int i = 0; i < errorListener.errorCount; i++) {
                errors.add("'" + errorListener.messages.get(i) + "' @ " + errorListener.positions.get(i));
            }

            throw new InvalidConfigurationException("Invalid expression '" + exprString + "' with " + errorListener.errorCount + " error(s): " + String.join(", ", errors));
        }

        return Operator.eval(null, expression(exprContext.children, ExpressionRules.LET), variables);
    }

    public TDL4Interpreter(ScriptHolder script) {
        CharStream cs = CharStreams.fromString(script.script);
        TDL4Lexicon lexer = new TDL4Lexicon(cs);
        TDL4 parser = new TDL4(new CommonTokenStream(lexer));

        TDL4ErrorListener errorListener = new TDL4ErrorListener();
        parser.addErrorListener(errorListener);

        scriptContext = parser.script();

        if (errorListener.errorCount > 0) {
            throw new InvalidConfigurationException("Invalid TDL4 script: " + errorListener.errorCount + " error(s). First error is '" + errorListener.messages.get(0)
                    + "' @ " + errorListener.lines.get(0) + ":" + errorListener.positions.get(0));
        }

        variables.putAll(script.variables);
        options = script.options;
    }

    public void initialize(DataContext dataContext) {
        for (TDL4.StatementContext stmt : scriptContext.statement()) {
            if (stmt.options_stmt() != null) {
                Map<String, Object> opts = resolveParams(stmt.options_stmt().params_expr());
                options.putAll(opts);
            }
        }

        this.dataContext = dataContext;
        dataContext.initialize(options);
    }

    public void interpret() {
        for (TDL4.StatementContext stmt : scriptContext.statement()) {
            statement(stmt);
        }
    }

    public Iterator<TDL4.StatementContext> iterator() {
        return scriptContext.statement().iterator();
    }

    private void statement(TDL4.StatementContext stmt) {
        if (stmt.create_stmt() != null) {
            create(stmt.create_stmt());
        }
        if (stmt.transform_stmt() != null) {
            transform(stmt.transform_stmt());
        }
        if (stmt.copy_stmt() != null) {
            copy(stmt.copy_stmt());
        }
        if (stmt.let_stmt() != null) {
            let(stmt.let_stmt());
        }
        if (stmt.loop_stmt() != null) {
            loop(stmt.loop_stmt());
        }
        if (stmt.if_stmt() != null) {
            ifElse(stmt.if_stmt());
        }
        if (stmt.select_stmt() != null) {
            select(stmt.select_stmt());
        }
        if (stmt.call_stmt() != null) {
            call(stmt.call_stmt());
        }
        if (stmt.analyze_stmt() != null) {
            analyze(stmt.analyze_stmt());
        }
    }

    private void create(TDL4.Create_stmtContext ctx) {
        String inputName = resolveIdLiteral(ctx.ds_name().L_IDENTIFIER());
        Map<String, Object> params = resolveParams(ctx.params_expr());

        Partitioning partitioning = Partitioning.HASHCODE;
        if (ctx.partition_by() != null) {
            if (ctx.partition_by().K_RANDOM() != null) {
                partitioning = Partitioning.RANDOM;
            }
            if (ctx.partition_by().K_SOURCE() != null) {
                partitioning = Partitioning.SOURCE;
            }
        }

        dataContext.createDataStream(inputName, params, partitioning);
    }

    private void transform(TDL4.Transform_stmtContext ctx) {
        String dsName = resolveIdLiteral(ctx.ds_name().L_IDENTIFIER());

        if (!dataContext.has(dsName)) {
            throw new InvalidConfigurationException("TRANSFORM \"" + dsName + "\" refers to nonexistent DataStream");
        }

        TDL4.Func_exprContext funcExpr = ctx.func_expr();

        String tfVerb = resolveIdLiteral(funcExpr.func().L_IDENTIFIER());
        if (!Transforms.TRANSFORMS.containsKey(tfVerb)) {
            throw new InvalidConfigurationException("TRANSFORM " + tfVerb + "() refers to unknown Transform");
        }

        TransformInfo tfInfo = Transforms.TRANSFORMS.get(tfVerb);
        TransformMeta meta = tfInfo.meta;

        StreamType from = dataContext.get(dsName).streamType;
        if ((meta.from != StreamType.Passthru) && (meta.from != from)) {
            throw new InvalidConfigurationException("TRANSFORM " + tfVerb + "() doesn't accept source DataStream type " + from);
        }

        Map<String, Object> params = resolveParams(funcExpr.params_expr());
        if (meta.definitions != null) {
            for (Map.Entry<String, DefinitionMeta> defEntry : meta.definitions.entrySet()) {
                String name = defEntry.getKey();
                DefinitionMeta def = defEntry.getValue();

                if (!def.optional && !params.containsKey(name)) {
                    throw new InvalidConfigurationException("TRANSFORM " + tfVerb + "() must have mandatory parameter @" + name + " set");
                }
            }
        }

        StreamType requested = meta.to;

        Map<String, List<String>> columns = new HashMap<>();
        for (TDL4.Columns_itemContext columnsItem : ctx.columns_item()) {
            List<String> columnList = columnsItem.L_IDENTIFIER().stream().map(this::resolveIdLiteral).collect(Collectors.toList());

            TDL4.Type_columnsContext columnsType = columnsItem.type_columns();

            switch (requested) {
                case PlainText:
                case Columnar:
                case Structured: {
                    if ((columnsType == null) || (columnsType.K_VALUE() != null)) {
                        columns.put(OBJLVL_VALUE, columnList);
                    }
                    break;
                }
                case Point: {
                    if ((columnsType.T_POINT() != null)) {
                        columns.put(OBJLVL_POINT, columnList);
                    }
                    break;
                }
                case Track: {
                    if (columnsType.T_TRACK() != null) {
                        columns.put(OBJLVL_TRACK, columnList);
                    } else if (columnsType.T_POINT() != null) {
                        columns.put(OBJLVL_POINT, columnList);
                    } else if (columnsType.T_SEGMENT() != null) {
                        columns.put(OBJLVL_SEGMENT, columnList);
                    }
                    break;
                }
                case Polygon: {
                    if (columnsType.T_POLYGON() != null) {
                        columns.put(OBJLVL_POLYGON, columnList);
                    }
                    break;
                }
            }
        }

        TDL4.Key_itemContext keyExpr = ctx.key_item();
        List<Expression<?>> keyExpression = (keyExpr == null) ? Collections.emptyList() : expression(keyExpr.expression().children, ExpressionRules.QUERY);

        StreamConverter converter;
        try {
            converter = tfInfo.configurable.getDeclaredConstructor().newInstance().converter();
        } catch (Exception e) {
            throw new InvalidConfigurationException("Unable to initialize TRANSFORM " + tfVerb + "()");
        }
        dataContext.alterDataStream(dsName, converter, columns, keyExpression, tfInfo.meta.keyAfter(), new Configuration(tfInfo.meta.definitions, "Transform '" + tfVerb + "'", params));
    }

    private void copy(TDL4.Copy_stmtContext ctx) {
        String outputName = ctx.ds_name().getText();

        dataContext.copyDataStream(outputName, ctx.S_STAR() != null, resolveParams(ctx.params_expr()));
    }

    private void let(TDL4.Let_stmtContext ctx) {
        String varName = ctx.var_name().L_IDENTIFIER().getText();

        Object value = null;
        if (ctx.array() != null) {
            value = resolveArray(ctx.array(), ExpressionRules.LET);
        }
        if (ctx.let_expr() != null) {
            value = Operator.eval(null, expression(ctx.let_expr().children, ExpressionRules.LET), variables);
        }
        if (ctx.sub_query() != null) {
            value = subQuery(ctx.sub_query()).toArray();
        }

        variables.put(varName, value);
    }

    private void loop(TDL4.Loop_stmtContext ctx) {
        String varName = resolveIdLiteral(ctx.var_name(0).L_IDENTIFIER());

        Object[] value;
        if (ctx.array() != null) {
            value = resolveArray(ctx.array(), ExpressionRules.LET);
        } else {
            value = variables.getArray(resolveIdLiteral(ctx.var_name(1).L_IDENTIFIER()));
        }

        boolean loop = (value != null) && (value.length > 0);

        if (loop) {
            variables = new VariablesContext(variables);

            for (Object val : value) {
                variables.put(varName, val);

                for (TDL4.StatementContext stmt : ctx.then_item().statement()) {
                    statement(stmt);
                }
            }

            variables = variables.parent;
        } else {
            if (ctx.else_item() != null) {
                for (TDL4.StatementContext stmt : ctx.else_item().statement()) {
                    statement(stmt);
                }
            }
        }
    }

    private void ifElse(TDL4.If_stmtContext ctx) {
        TDL4.Let_exprContext expr = ctx.let_expr();

        boolean then = Operator.bool(null, expression(expr.children, ExpressionRules.LET), variables);
        if (then) {
            for (TDL4.StatementContext stmt : ctx.then_item().statement()) {
                statement(stmt);
            }
        } else {
            if (ctx.else_item() != null) {
                for (TDL4.StatementContext stmt : ctx.else_item().statement()) {
                    statement(stmt);
                }
            }
        }
    }

    private List<Expression<?>> expression(List<ParseTree> exprChildren, ExpressionRules rules) {
        List<Expression<?>> items = new ArrayList<>();

        Deque<ParseTree> whereOpStack = new LinkedList<>();
        List<ParseTree> predExpStack = new ArrayList<>();
        int i = 0;
        // doing Shunting Yard
        for (; i < exprChildren.size(); i++) {
            ParseTree child = exprChildren.get(i);

            if ((child instanceof TDL4.Expression_opContext)
                    || (child instanceof TDL4.Comparison_opContext)
                    || (child instanceof TDL4.Bool_opContext)
                    || (child instanceof TDL4.In_opContext)
                    || (child instanceof TDL4.Is_opContext)
                    || (child instanceof TDL4.Between_opContext)
                    || (child instanceof TDL4.Digest_opContext)
                    || (child instanceof TDL4.Default_opContext)) {
                while (!whereOpStack.isEmpty()) {
                    ParseTree peek = whereOpStack.peek();

                    if (peek instanceof TerminalNode) {
                        TerminalNode tn = (TerminalNode) peek;
                        int tt = tn.getSymbol().getType();
                        if (tt == TDL4Lexicon.S_OPEN_PAR) {
                            break;
                        }
                    }
                    if (isHigher(child, peek)) {
                        predExpStack.add(whereOpStack.pop());
                    } else {
                        break;
                    }
                }

                whereOpStack.push(child);
                continue;
            }

            if (child instanceof TerminalNode) {
                TerminalNode tn = (TerminalNode) child;
                int tt = tn.getSymbol().getType();
                if (tt == TDL4Lexicon.S_OPEN_PAR) {
                    whereOpStack.add(child);
                    continue;
                }

                if (tt == TDL4Lexicon.S_CLOSE_PAR) {
                    while (true) {
                        if (whereOpStack.isEmpty()) {
                            throw new RuntimeException("Mismatched parentheses at query token #" + i);
                        }
                        ParseTree pop = whereOpStack.pop();
                        if (!(pop instanceof TerminalNode)) {
                            predExpStack.add(pop);
                        } else {
                            break;
                        }
                    }
                    continue;
                }
            }

            // expression
            predExpStack.add(child);
        }

        while (!whereOpStack.isEmpty()) {
            predExpStack.add(whereOpStack.pop());
        }

        for (ParseTree exprItem : predExpStack) {
            if (exprItem instanceof TDL4.Property_nameContext) {
                switch (rules) {
                    case QUERY: {
                        String propName = parseIdentifier(exprItem.getText());

                        items.add(Expressions.propItem(propName));
                        continue;
                    }
                    case AT: {
                        String propName = parseIdentifier(exprItem.getText());

                        items.add(Expressions.stringItem(propName));
                        continue;
                    }
                    default: {
                        throw new InvalidConfigurationException("Attribute name is not allowed in this context: " + exprItem.getText());
                    }
                }
            }

            if (exprItem instanceof TDL4.Var_nameContext) {
                TDL4.Var_nameContext varNameCtx = (TDL4.Var_nameContext) exprItem;

                String varName = resolveIdLiteral(varNameCtx.L_IDENTIFIER());

                items.add(Expressions.varItem(varName));
                continue;
            }

            if (exprItem instanceof TDL4.Between_opContext) {
                TDL4.Between_opContext between = (TDL4.Between_opContext) exprItem;

                items.add(Expressions.stackGetter(1));

                double l = parseNumber(between.L_NUMERIC(0).getText()).doubleValue();
                double r = parseNumber(between.L_NUMERIC(1).getText()).doubleValue();
                items.add((between.K_NOT() == null)
                        ? Expressions.between(l, r)
                        : Expressions.notBetween(l, r)
                );

                continue;
            }

            if (exprItem instanceof TDL4.In_opContext) {
                TDL4.In_opContext inCtx = (TDL4.In_opContext) exprItem;
                if (inCtx.array() != null) {
                    items.add(Expressions.setItem(resolveArray(inCtx.array(), ExpressionRules.QUERY)));
                }
                if (inCtx.var_name() != null) {
                    items.add(Expressions.arrItem(resolveIdLiteral(inCtx.var_name().L_IDENTIFIER())));
                }
                if (inCtx.property_name() != null) {
                    items.add(Expressions.propItem(parseIdentifier(inCtx.property_name().getText())));
                }

                items.add(Expressions.stackGetter(2));

                boolean not = inCtx.K_NOT() != null;
                items.add(not ? Expressions.notIn() : Expressions.in());

                continue;
            }

            // column_name IS NOT? NULL
            if (exprItem instanceof TDL4.Is_opContext) {
                items.add(Expressions.stackGetter(1));

                items.add((((TDL4.Is_opContext) exprItem).K_NOT() == null) ? Expressions.isNull() : Expressions.nonNull());

                continue;
            }

            if ((exprItem instanceof TDL4.Expression_opContext)
                    || (exprItem instanceof TDL4.Comparison_opContext)
                    || (exprItem instanceof TDL4.Bool_opContext)
                    || (exprItem instanceof TDL4.Digest_opContext)
                    || (exprItem instanceof TDL4.Default_opContext)) {
                Operator eo = Operator.get(exprItem.getText());
                if (eo == null) {
                    throw new RuntimeException("Unknown operator token " + exprItem.getText());
                } else {
                    items.add(Expressions.stackGetter(eo.ariness));
                    items.add(Expressions.opItem(eo));
                }

                continue;
            }

            TerminalNode tn = (TerminalNode) exprItem;
            int type = tn.getSymbol().getType();
            if (type == TDL4Lexicon.L_NUMERIC) {
                items.add(Expressions.numericItem(parseNumber(tn.getText())));
                continue;
            }
            if (type == TDL4Lexicon.L_STRING) {
                items.add(Expressions.stringItem(parseString(tn.getText())));
                continue;
            }
            if (type == TDL4Lexicon.K_NULL) {
                items.add(Expressions.nullItem());
                continue;
            }
            if ((type == TDL4Lexicon.K_TRUE) || (type == TDL4Lexicon.K_FALSE)) {
                items.add(Expressions.boolItem(Boolean.parseBoolean(tn.getText())));
                continue;
            }
        }

        return items;
    }

    private void select(TDL4.Select_stmtContext ctx) {
        String intoName = parseIdentifier(ctx.ds_name().L_IDENTIFIER().getText());
        if (dataContext.has(intoName)) {
            throw new InvalidConfigurationException("SELECT INTO \"" + intoName + "\" tries to create DataStream \"" + intoName + "\" which already exists");
        }

        TDL4.From_scopeContext from = ctx.from_scope();

        boolean distinct = ctx.K_DISTINCT() != null;

        JoinSpec join = null;
        TDL4.Join_opContext joinCtx = from.join_op();
        if (joinCtx != null) {
            if (joinCtx.K_LEFT() != null) {
                join = (joinCtx.K_ANTI() != null) ? JoinSpec.LEFT_ANTI : JoinSpec.LEFT;
            } else if (joinCtx.K_RIGHT() != null) {
                join = (joinCtx.K_ANTI() != null) ? JoinSpec.RIGHT_ANTI : JoinSpec.RIGHT;
            } else if (joinCtx.K_OUTER() != null) {
                join = JoinSpec.OUTER;
            } else {
                join = JoinSpec.INNER;
            }
        }

        boolean starFrom = false;
        UnionSpec union = null;
        if (from.union_op() != null) {
            if (from.S_STAR() != null) {
                starFrom = true;
            }

            if (from.union_op().K_XOR() != null) {
                union = UnionSpec.XOR;
            } else if (from.union_op().K_AND() != null) {
                union = UnionSpec.AND;
            } else {
                union = UnionSpec.CONCAT;
            }
        }

        List<String> fromSet = from.ds_name().stream().map(e -> resolveIdLiteral(e.L_IDENTIFIER())).collect(Collectors.toList());
        if (starFrom) {
            fromSet = dataContext.getAll(fromSet.get(0) + "*").keyList();
        }

        List<SelectItem> items = new ArrayList<>();

        DataStream firstStream = dataContext.get(fromSet.get(0));

        List<String> columns = new ArrayList<>();
        boolean star = (ctx.S_STAR() != null);
        if (star) {
            if (join != null) {
                for (String fromName : fromSet) {
                    columns.addAll(dataContext.get(fromName).accessor.attributes(OBJLVL_VALUE));
                }
            } else {
                columns.addAll(firstStream.accessor.attributes(OBJLVL_VALUE));
            }
        } else {
            List<TDL4.What_exprContext> what = ctx.what_expr();

            for (TDL4.What_exprContext expr : what) {
                TDL4.AliasContext aliasCtx = expr.alias();
                String alias = (aliasCtx != null) ? resolveIdLiteral(aliasCtx.L_IDENTIFIER()) : expr.expression().getText();
                columns.add(alias);
                List<Expression<?>> item = expression(expr.expression().children, ExpressionRules.QUERY);
                String typeAlias = resolveType(expr.type_alias());
                items.add(new SelectItem(item, alias, typeAlias));
            }
        }

        Long limitRecords = null;
        Double limitPercent = null;

        if (ctx.limit_expr() != null) {
            if (ctx.limit_expr().S_PERCENT() != null) {
                limitPercent = resolveNumericLiteral(ctx.limit_expr().L_NUMERIC()).doubleValue();
                if ((limitPercent <= 0) || (limitPercent > 100)) {
                    throw new RuntimeException("Percentage in LIMIT clause can't be 0 or less and more than 100");
                }
            } else {
                limitRecords = resolveNumericLiteral(ctx.limit_expr().L_NUMERIC()).longValue();
                if (limitRecords <= 0) {
                    throw new RuntimeException("Record number in LIMIT clause can't be 0 or less");
                }
            }
        }

        WhereItem whereItem = new WhereItem();
        TDL4.Where_exprContext whereCtx = ctx.where_expr();
        if (whereCtx != null) {
            List<Expression<?>> expr = expression(whereCtx.expression().children, ExpressionRules.QUERY);
            String category = resolveType(whereCtx.type_alias());
            whereItem = new WhereItem(expr, category);
        }

        if (star && (union == null) && (join == null) && (whereItem.expression == null)) {
            dataContext.put(intoName, new DataStream(firstStream.streamType, firstStream.rdd, firstStream.accessor.attributes()));
        } else {
            JavaPairRDD<Object, Record<?>> result = dataContext.select(distinct, fromSet, union, join, star, items, whereItem, limitPercent, limitRecords, variables);
            dataContext.put(intoName, new DataStream(firstStream.streamType, result, Collections.singletonMap(OBJLVL_VALUE, columns)));
        }
    }

    private Collection<Object> subQuery(TDL4.Sub_queryContext subQuery) {
        boolean distinct = subQuery.K_DISTINCT() != null;

        String input = subQuery.ds_name().getText();

        TDL4.What_exprContext what = subQuery.what_expr();
        List<Expression<?>> item = expression(what.expression().children, ExpressionRules.QUERY);

        List<Expression<?>> query = new ArrayList<>();
        if (subQuery.where_expr() != null) {
            query = expression(subQuery.where_expr().expression().children, ExpressionRules.QUERY);
        }

        Long limitRecords = null;
        Double limitPercent = null;

        if (subQuery.limit_expr() != null) {
            if (subQuery.limit_expr().S_PERCENT() != null) {
                limitPercent = resolveNumericLiteral(subQuery.limit_expr().L_NUMERIC()).doubleValue();
                if ((limitPercent <= 0) || (limitPercent > 100)) {
                    throw new RuntimeException("Percentage in LIMIT clause can't be 0 or less and more than 100");
                }
            } else {
                limitRecords = resolveNumericLiteral(subQuery.limit_expr().L_NUMERIC()).longValue();
                if (limitRecords <= 0) {
                    throw new RuntimeException("Record number in LIMIT clause can't be 0 or less");
                }
            }
        }

        if (dataContext.has(input)) {
            return dataContext.subQuery(distinct, dataContext.get(input), item, query, limitPercent, limitRecords, variables);
        } else {
            throw new InvalidConfigurationException("LET with SELECT refers to nonexistent DataStream \"" + input + "\"");
        }
    }

    private void call(TDL4.Call_stmtContext ctx) {
        TDL4.Func_exprContext funcExpr = ctx.func_expr();

        String opVerb = resolveIdLiteral(funcExpr.func().L_IDENTIFIER());
        if (!Operations.OPERATIONS.containsKey(opVerb)) {
            throw new InvalidConfigurationException("CALL \"" + opVerb + "\"() refers to unknown Operation");
        }

        OperationInfo opInfo = Operations.OPERATIONS.get(opVerb);
        OperationMeta meta = opInfo.meta;
        Map<String, Object> params = resolveParams(funcExpr.params_expr());
        if (meta.definitions != null) {
            for (Map.Entry<String, DefinitionMeta> defEntry : meta.definitions.entrySet()) {
                String name = defEntry.getKey();
                DefinitionMeta def = defEntry.getValue();

                if (!def.optional && !params.containsKey(name)) {
                    throw new InvalidConfigurationException("CALL \"" + opVerb + "\"() must have mandatory parameter @" + name + " set");
                }
            }
        }

        ListOrderedMap<String, DataStream> inputMap;
        TDL4.From_namedContext fromNamed = ctx.from_named();
        if (meta.input instanceof PositionalStreamsMeta) {
            PositionalStreamsMeta psm = (PositionalStreamsMeta) meta.input;

            TDL4.From_positionalContext fromScope = ctx.from_positional();
            if (fromScope == null) {
                throw new InvalidConfigurationException("CALL \"" + opVerb + "\" INPUT FROM requires positional or wildcard DataStream references");
            }

            String dsNames = (fromScope.S_STAR() != null) ? resolveIdLiteral(fromScope.ds_name(0).L_IDENTIFIER()) + Constants.STAR
                    : fromScope.ds_name().stream().map(dsn -> resolveIdLiteral(dsn.L_IDENTIFIER())).collect(Collectors.joining(Constants.COMMA));

            inputMap = dataContext.getAll(dsNames);
            String foundStreams = inputMap.keySet()
                    .stream().map(l -> "\"" + l + "\"").collect(Collectors.joining(Constants.COMMA));

            if ((psm.positional > 0) && (inputMap.size() != psm.positional)) {
                throw new InvalidConfigurationException("CALL \"" + opVerb + "\" INPUT FROM requires exactly " + psm.positional + " positional DataStream reference(s)");
            }

            for (DataStream ds : inputMap.values()) {
                final StreamType _ct = ds.streamType;
                Stream.of(psm.streams.type).filter(st -> st == _ct).findFirst()
                        .orElseThrow(() -> new InvalidConfigurationException("CALL \"" + opVerb + "\" INPUT FROM " + foundStreams +
                                " has positional DataStreams of unexpected type " + _ct.name()));
            }
        } else {
            NamedStreamsMeta nsm = (NamedStreamsMeta) meta.input;

            if (fromNamed == null) {
                throw new InvalidConfigurationException("CALL \"" + opVerb + "\" INPUT FROM requires aliased DataStream references");
            }

            LinkedHashMap<String, String> dsMappings = new LinkedHashMap<>();
            List<TDL4.Ds_nameContext> ds_name = fromNamed.ds_name();
            for (int i = 0; i < ds_name.size(); i++) {
                dsMappings.put(parseIdentifier(fromNamed.ds_alias(i).L_IDENTIFIER().getText()),
                        parseIdentifier(ds_name.get(i).L_IDENTIFIER().getText()));
            }

            for (Map.Entry<String, DataStreamMeta> ns : nsm.streams.entrySet()) {
                DataStreamMeta dsm = ns.getValue();

                String alias = ns.getKey();
                if (!dsm.optional && !dsMappings.containsKey(alias)) {
                    throw new InvalidConfigurationException("CALL \"" + opVerb + "\" INPUT FROM requires aliased DataStream reference AS \"" + alias + "\", but it wasn't supplied");
                }
            }
            inputMap = new ListOrderedMap<>();
            for (Map.Entry<String, String> dsm : dsMappings.entrySet()) {
                String alias = dsm.getKey();
                if (!nsm.streams.containsKey(alias)) {
                    throw new InvalidConfigurationException("CALL \"" + opVerb + "\" INPUT FROM refers to unknown DataStream alias AS \"" + alias + "\"");
                }

                inputMap.put(alias, dataContext.get(dsm.getValue()));
            }
        }

        ListOrderedMap<String, String> outputMap = new ListOrderedMap<>();
        if (meta.output instanceof PositionalStreamsMeta) {
            TDL4.Into_positionalContext intoScope = ctx.into_positional();
            if (intoScope == null) {
                throw new InvalidConfigurationException("CALL \"" + opVerb + "\" OUTPUT INTO requires positional DataStream reference");
            }

            if (intoScope.S_STAR() != null) {
                String prefix = resolveIdLiteral(intoScope.ds_name(0).L_IDENTIFIER());

                inputMap.keyList().stream().map(e -> prefix + e).forEach(e -> outputMap.put(e, e));
            } else {
                intoScope.ds_name().stream().map(dsn -> resolveIdLiteral(dsn.L_IDENTIFIER())).forEach(e -> outputMap.put(e, e));
            }

            PositionalStreamsMeta psm = (PositionalStreamsMeta) meta.output;
            if ((psm.positional > 0) && (outputMap.size() != psm.positional)) {
                throw new InvalidConfigurationException("CALL \"" + opVerb + "\" OUTPUT INTO requires exactly " + psm.positional + " positional DataStream reference(s)");
            }

            for (String outputName : outputMap.values()) {
                if (dataContext.has(outputName)) {
                    throw new InvalidConfigurationException("CALL \"" + opVerb + "\" OUTPUT INTO tries to create DataStream \"" + outputName + "\" which already exists");
                }
            }
        } else {
            NamedStreamsMeta nsm = (NamedStreamsMeta) meta.output;

            TDL4.Into_namedContext intoScope = ctx.into_named();
            if (intoScope == null) {
                throw new InvalidConfigurationException("CALL \"" + opVerb + "\" OUTPUT INTO requires aliased DataStream references");
            }

            List<TDL4.Ds_nameContext> dsNames = intoScope.ds_name();
            List<TDL4.Ds_aliasContext> dsAliases = intoScope.ds_alias();
            for (int i = 0; i < dsNames.size(); i++) {
                outputMap.put(parseIdentifier(dsAliases.get(i).L_IDENTIFIER().getText()), parseIdentifier(dsNames.get(i).L_IDENTIFIER().getText()));
            }
            for (String outputName : outputMap.values()) {
                if (dataContext.has(outputName)) {
                    throw new InvalidConfigurationException("CALL \"" + opVerb + "\" OUTPUT INTO tries to create DataStream \"" + outputName + "\" which already exists");
                }
            }

            for (Map.Entry<String, DataStreamMeta> ns : nsm.streams.entrySet()) {
                DataStreamMeta dsm = ns.getValue();

                String dsAlias = ns.getKey();
                if (!dsm.optional && !outputMap.containsKey(dsAlias)) {
                    throw new InvalidConfigurationException("CALL \"" + opVerb + "\" OUTPUT INTO requires aliased DataStream reference AS \"" + dsAlias + "\", but it wasn't supplied");
                }
            }
        }

        Map<String, DataStream> result;
        try {
            Operation op = opInfo.configurable.getDeclaredConstructor().newInstance();
            op.initialize(dataContext.getUtils(), inputMap, new Configuration(opInfo.meta.definitions, "Operation '" + opVerb + "'", params), outputMap);
            result = op.execute();
        } catch (Exception e) {
            throw new InvalidConfigurationException("CALL \"" + opVerb + "\" failed with an exception", e);
        }

        for (Map.Entry<String, DataStream> output : result.entrySet()) {
            String outputName = output.getKey();
            if (dataContext.has(outputName)) {
                throw new InvalidConfigurationException("CALL \"" + opVerb + "\" OUTPUT INTO tries to create DataStream \"" + outputName + "\" which already exists");
            } else {
                dataContext.put(outputName, output.getValue());
            }
        }
    }

    private void analyze(TDL4.Analyze_stmtContext ctx) {
        String dsName = ctx.ds_name().getText();
        if (ctx.S_STAR() != null) {
            dsName += Constants.STAR;
        }
        String counterColumn = (ctx.K_KEY() == null) ? null
                : parseIdentifier(ctx.property_name().getText());

        List<Tuple2<Object, Record<?>>> metricsList = new ArrayList<>();
        for (Map.Entry<String, DataStream> e : dataContext.getAll(dsName).entrySet()) {
            String streamName = e.getKey();
            DataStream ds = e.getValue();

            List<String> columns = ds.accessor.attributes(OBJLVL_VALUE);

            final String _counterColumn = columns.contains(counterColumn) ? counterColumn : null;

            JavaPairRDD<Object, Record<?>> inputRdd = ds.rdd;
            JavaPairRDD<Object, Object> rdd2 = inputRdd.mapPartitionsToPair(it -> {
                List<Tuple2<Object, Object>> ret = new ArrayList<>();
                while (it.hasNext()) {
                    Tuple2<Object, Record<?>> r = it.next();

                    Object id;
                    if (_counterColumn == null) {
                        id = r._1;
                    } else {
                        id = r._2.asIs(_counterColumn);
                    }

                    ret.add(new Tuple2<>(id, null));
                }

                return ret.iterator();
            });

            List<Long> counts = rdd2
                    .aggregateByKey(0L, (c, v) -> c + 1L, Long::sum)
                    .values()
                    .sortBy(t -> t, true, 1)
                    .collect();

            int uniqueCounters = counts.size();
            String streamType = ds.streamType.name();
            long totalCount = counts.stream().reduce(Long::sum).orElse(0L);
            double counterAverage = (uniqueCounters == 0) ? 0.D : ((double) totalCount / uniqueCounters);
            double counterMedian = 0.D;
            if (uniqueCounters != 0) {
                int m = (uniqueCounters <= 2) ? 0 : (uniqueCounters >> 1);
                counterMedian = ((uniqueCounters % 2) == 0) ? (counts.get(m) + counts.get(m + 1)) / 2.D : counts.get(m).doubleValue();
            }

            Columnar rec = new Columnar(METRICS_COLUMNS, new Object[]{streamName, streamType, counterColumn, totalCount, uniqueCounters, counterAverage, counterMedian});
            metricsList.add(new Tuple2<>(_counterColumn, rec));
        }

        metricsList.addAll(dataContext.get(METRICS_DS).rdd.collect());
        dataContext.put(Constants.METRICS_DS, new DataStream(StreamType.Columnar,
                dataContext.utils.parallelizePairs(metricsList, 1),
                Collections.singletonMap(OBJLVL_VALUE, METRICS_COLUMNS)));
    }

    public Map<String, Object> resolveParams(TDL4.Params_exprContext params) {
        Map<String, Object> ret = new HashMap<>();

        if (params != null) {
            for (TDL4.ParamContext atRule : params.param()) {
                Object obj = null;
                if (atRule.array() != null) {

                    obj = resolveArray(atRule.array(), ExpressionRules.AT);
                } else {
                    obj = Operator.eval(null, expression(atRule.expression().children, ExpressionRules.AT), variables);
                }

                ret.put(parseIdentifier(atRule.L_IDENTIFIER().getText()), obj);
            }
        }

        return ret;
    }

    private boolean isHigher(ParseTree o1, ParseTree o2) {
        Operator first = Operator.get(o1.getText());
        if (o1 instanceof TDL4.In_opContext) {
            first = Operator.IN;
        }
        if (o1 instanceof TDL4.Is_opContext) {
            first = Operator.IS;
        }
        if (o1 instanceof TDL4.Between_opContext) {
            first = Operator.BETWEEN;
        }

        Operator second = Operator.get(o2.getText());
        if (o2 instanceof TDL4.In_opContext) {
            second = Operator.IN;
        }
        if (o2 instanceof TDL4.Is_opContext) {
            second = Operator.IS;
        }
        if (o2 instanceof TDL4.Between_opContext) {
            second = Operator.BETWEEN;
        }

        return ((second.prio - first.prio) > 0) || ((first.prio == second.prio) && !first.rightAssoc);
    }

    private Object[] resolveArray(TDL4.ArrayContext array, ExpressionRules rules) {
        if (array != null) {
            if (!array.L_NUMERIC().isEmpty()) {
                return array.L_NUMERIC().stream()
                        .map(this::resolveNumericLiteral)
                        .toArray(Number[]::new);
            }
            if (!array.L_STRING().isEmpty()) {
                return array.L_STRING().stream()
                        .map(this::resolveStringLiteral)
                        .toArray(String[]::new);
            }
            if (rules == ExpressionRules.AT) {
                if (!array.L_IDENTIFIER().isEmpty()) {
                    return array.L_IDENTIFIER().stream()
                            .map(this::resolveIdLiteral)
                            .toArray(String[]::new);
                }
            }
        }
        return null;
    }

    private Number resolveNumericLiteral(TerminalNode numericLiteral) {
        if (numericLiteral != null) {
            return parseNumber(numericLiteral.getText());
        }
        return null;
    }

    private String resolveStringLiteral(TerminalNode stringLiteral) {
        if (stringLiteral != null) {
            String s = stringLiteral.getText();
            return parseString(s);
        }
        return null;
    }

    private String resolveIdLiteral(TerminalNode idLiteral) {
        if (idLiteral != null) {
            String s = idLiteral.getText();
            return parseIdentifier(s);
        }
        return null;
    }

    private String resolveType(TDL4.Type_aliasContext type_aliasContext) {
        if (type_aliasContext != null) {
            if (type_aliasContext.T_TRACK() != null) {
                return OBJLVL_TRACK;
            }
            if (type_aliasContext.T_SEGMENT() != null) {
                return OBJLVL_SEGMENT;
            }
            if (type_aliasContext.T_POINT() != null) {
                return OBJLVL_POINT;
            }
            if (type_aliasContext.T_POLYGON() != null) {
                return OBJLVL_POLYGON;
            }
        }
        return OBJLVL_VALUE;
    }

    private enum ExpressionRules {
        LET,
        AT,
        QUERY
    }
}
