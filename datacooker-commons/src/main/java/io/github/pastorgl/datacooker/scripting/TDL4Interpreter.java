/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.scripting;

import io.github.pastorgl.datacooker.Options;
import io.github.pastorgl.datacooker.commons.functions.ArrayFunctions;
import io.github.pastorgl.datacooker.config.*;
import io.github.pastorgl.datacooker.data.*;
import io.github.pastorgl.datacooker.metadata.*;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.commons.collections4.map.ListOrderedMap;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Function1;

import java.util.*;
import java.util.stream.Collectors;

import static io.github.pastorgl.datacooker.Constants.CWD_VAR;
import static io.github.pastorgl.datacooker.Options.loop_iteration_limit;
import static io.github.pastorgl.datacooker.Options.loop_nesting_limit;

public class TDL4Interpreter {
    private final String script;

    private DataContext dataContext;

    private final OptionsContext options;
    private final boolean verbose;
    private int stCnt = 0;

    private VariablesContext variables;

    private final TDL4ErrorListener errorListener;
    private CommonTokenStream tokenStream;
    private TDL4.ScriptContext scriptContext;

    private final Library library;

    private String interpretString(String interp) {
        int opBr = interp.indexOf('{');
        if (opBr >= 0) {
            if ((opBr == 0) || (interp.charAt(opBr - 1) != '\\')) {
                int clBr = interp.indexOf('}', opBr);

                while (clBr >= 0) {
                    if (interp.charAt(clBr - 1) != '\\') {
                        interp = interp.substring(0, opBr) + interpretExpr(interp.substring(opBr + 1, clBr)) + interpretString(interp.substring(clBr + 1));
                        break;
                    } else {
                        clBr = interp.indexOf('}', clBr + 1);
                    }
                }
            } else {
                interp = interp.substring(0, opBr + 1) + interpretString(interp.substring(opBr + 1));
            }
        }

        return interp.replace("\\{", "{").replace("\\}", "}");
    }

    private Object interpretExpr(String exprString) {
        return new TDL4Interpreter(library, exprString, variables, options, errorListener).interpretExpr();
    }

    public Object interpretExpr() {
        if (script.isEmpty()) {
            return "";
        }

        CharStream cs = CharStreams.fromString(script);

        TDL4Lexicon lexer = new TDL4Lexicon(cs);
        lexer.removeErrorListeners();
        lexer.addErrorListener(errorListener);

        TDL4 parser = new TDL4(new CommonTokenStream(lexer));
        parser.removeErrorListeners();
        parser.addErrorListener(errorListener);

        TDL4.Loose_expressionContext exprContext = parser.loose_expression();

        if (errorListener.errorCount > 0) {
            List<String> errors = new ArrayList<>();
            for (int i = 0; i < errorListener.errorCount; i++) {
                errors.add("'" + errorListener.messages.get(i) + "' @ " + errorListener.positions.get(i));
            }

            throw new InvalidConfigurationException("Invalid expression '" + script + "' with " + errorListener.errorCount + " error(s): " + String.join(", ", errors));
        }

        return Expressions.eval(null, null, expression(exprContext.expression().children, ExpressionRules.LOOSE), variables);
    }

    public TDL4Interpreter(Library library, String script, VariablesContext variables, OptionsContext options, TDL4ErrorListener errorListener) {
        this.library = library;
        this.script = script;
        this.variables = variables;
        this.options = options;
        this.errorListener = errorListener;

        verbose = options.getBoolean(Options.batch_verbose.name(), Options.batch_verbose.def());
    }

    public void parseScript() {
        CharStream cs = CharStreams.fromString(script);

        TDL4Lexicon lexer = new TDL4Lexicon(cs);
        lexer.removeErrorListeners();
        lexer.addErrorListener(errorListener);
        tokenStream = new CommonTokenStream(lexer);

        TDL4 parser = new TDL4(tokenStream);
        parser.removeErrorListeners();
        parser.addErrorListener(errorListener);
        scriptContext = parser.script();
    }

    public void interpret(DataContext dataContext) {
        if (scriptContext == null) {
            parseScript();
        }

        if (verbose) {
            System.out.println("-------------- OPTIONS ---------------");
            System.out.println("Before: " + options + "\n");
        }

        int o = 0;
        for (TDL4.StatementContext stmt : scriptContext.statements().statement()) {
            if (stmt.options_stmt() != null) {
                if (verbose) {
                    o++;
                    System.out.println(String.format("Change %05d parsed as: ", o) + stmtTokens(stmt) + "\n");
                }

                Map<String, Object> opts = resolveParams(stmt.options_stmt().params_expr());
                options.putAll(opts);
            }
        }

        if (verbose) {
            if (o > 0) {
                System.out.println("After " + o + " changes: " + options + "\n");
            } else {
                System.out.println("Unchanged\n");
            }
        }

        this.dataContext = dataContext;
        dataContext.initialize(options);

        for (TDL4.StatementContext stmt : scriptContext.statements().statement()) {
            statement(stmt);
        }
    }

    private String stmtTokens(ParserRuleContext stmt) {
        return tokenStream.getTokens(stmt.getStart().getTokenIndex(), stmt.getStop().getTokenIndex()).stream()
                .map(Token::getText)
                .filter(s -> !s.isBlank())
                .collect(Collectors.joining(" "));
    }

    private void statement(TDL4.StatementContext stmt) {
        if (verbose) {
            System.out.printf("---------- STATEMENT #%05d ----------%n", ++stCnt);
            System.out.println("Parsed as: " + stmtTokens(stmt) + "\n");
        }

        if (stmt.create_stmt() != null) {
            create(stmt.create_stmt());
        }
        if (stmt.alter_stmt() != null) {
            alter(stmt.alter_stmt());
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
        if (stmt.create_proc() != null) {
            createProcedure(stmt.create_proc());
        }
        if (stmt.create_func() != null) {
            createFunction(stmt.create_func());
        }
        if (stmt.raise_stmt() != null) {
            raise(stmt.raise_stmt());
        }
        if (stmt.drop_stmt() != null) {
            drop(stmt.drop_stmt());
        }
    }

    private String defParams(final Map<String, DefinitionMeta> meta, final Map<String, Object> params) {
        if (meta == null) {
            return "NONE";
        }

        Function1<Object, String> pretty = (a) -> (a == null) ? "NULL" : (a.getClass().isArray() ? Arrays.toString((Object[]) a) : String.valueOf(a));

        List<String> sl = new LinkedList<>();
        sl.add(params.size() + " set");
        for (Map.Entry<String, DefinitionMeta> m : meta.entrySet()) {
            DefinitionMeta mm = m.getValue();
            String key = m.getKey();

            if (mm.dynamic) {
                int[] ii = {0};
                params.keySet().stream().filter(k -> k.startsWith(key)).forEach(k -> {
                    sl.add(key + " " + k.substring(key.length()) + ": " + pretty.apply(params.get(k)));
                    ii[0]++;
                });
                if (ii[0] == 0) {
                    sl.add(key + " not set");
                }
            } else if (mm.optional) {
                if (params.containsKey(key)) {
                    sl.add(key + " set to: " + pretty.apply(params.get(key)));
                } else {
                    sl.add(key + " defaults to: " + pretty.apply(mm.defaults));
                }
            } else {
                sl.add(key + " (required): " + pretty.apply(params.get(key)));
            }
        }
        return String.join("\n\t", sl);
    }

    private void create(TDL4.Create_stmtContext ctx) {
        String inputName = resolveName(ctx.ds_name().L_IDENTIFIER());

        if (dataContext.has(inputName)) {
            throw new InvalidConfigurationException("Can't CREATE DS \"" + inputName + "\", because it is already defined");
        }

        TDL4.Func_exprContext funcExpr = ctx.func_expr();
        String inVerb = resolveName(funcExpr.func().L_IDENTIFIER());

        if (!Pluggables.INPUTS.containsKey(inVerb)) {
            throw new InvalidConfigurationException("Storage input adapter \"" + inVerb + "\" isn't present");
        }

        Partitioning partitioning = (ctx.K_BY() != null) ? Partitioning.get(ctx.partition_by().getText()) : Partitioning.HASHCODE;

        int partCount = 1;
        if (ctx.ds_parts() != null) {
            Object parts = Expressions.eval(null, null, expression(ctx.ds_parts().expression().children, ExpressionRules.LOOSE), variables);
            partCount = (parts instanceof Number) ? (int) parts : Utils.parseNumber(String.valueOf(parts)).intValue();
            if (partCount < 1) {
                throw new InvalidConfigurationException("CREATE DS \"" + inputName + "\" requested number of PARTITIONs below 1");
            }
        }

        String path = String.valueOf(Expressions.eval(null, null, expression(ctx.expression().children, ExpressionRules.LOOSE), variables));

        PluggableMeta meta = Pluggables.INPUTS.get(inVerb).meta;

        Map<String, Object> params = resolveParams(funcExpr.params_expr());
        if (verbose) {
            System.out.println("CREATE parameters: " + defParams(meta.definitions, params) + "\n");
        }
        checkMeta(inVerb, params, meta);

        StreamType requested = ((OutputMeta) meta.output).type.types[0];
        Map<ObjLvl, List<String>> columns = getColumns(ctx.columns_item(), requested);

        ListOrderedMap<String, StreamInfo> si = dataContext.createDataStreams(inVerb, inputName, path, ctx.S_STAR() != null, params, columns, partCount, partitioning);

        if (verbose) {
            int ut = DataContext.usageThreshold();
            for (Map.Entry<String, StreamInfo> sii : si.entrySet()) {
                System.out.println("CREATEd DS " + sii.getKey() + ": " + sii.getValue().describe(ut));
            }
        }
    }

    private void alter(TDL4.Alter_stmtContext ctx) {
        String dsNames = resolveName(ctx.ds_name().L_IDENTIFIER());

        List<String> dataStreams;
        if (ctx.S_STAR() != null) {
            dataStreams = dataContext.getStarNames(dsNames);
        } else {
            if (dataContext.has(dsNames)) {
                dataStreams = Collections.singletonList(dsNames);
            } else {
                throw new InvalidConfigurationException("ALTER \"" + dsNames + "\" refers to nonexistent DataStream");
            }
        }

        TDL4.Func_exprContext funcExpr = ctx.func_expr();

        Map<ObjLvl, List<String>> columns = null;
        PluggableMeta meta = null;
        Map<String, Object> params = null;
        if (funcExpr != null) {
            String tfVerb = resolveName(funcExpr.func().L_IDENTIFIER());
            if (!Pluggables.TRANSFORMS.containsKey(tfVerb)) {
                throw new InvalidConfigurationException("Unknown Transform " + tfVerb);
            }

            meta = Pluggables.TRANSFORMS.get(tfVerb).meta;

            for (String dsName : dataStreams) {
                StreamType from = dataContext.get(dsName).streamType;
                StreamType accepts = ((InputMeta) meta.input).type.types[0];
                if ((accepts != StreamType.Passthru) && (accepts != from)) {
                    throw new InvalidConfigurationException("Transform " + tfVerb + " doesn't accept source DataStream type " + from);
                }
            }

            params = resolveParams(funcExpr.params_expr());
            if (verbose) {
                System.out.println("Transform parameters: " + defParams(meta.definitions, params) + "\n");
            }
            checkMeta(tfVerb, params, meta);

            StreamType requested = ((OutputMeta) meta.output).type.types[0];
            columns = getColumns(ctx.columns_item(), requested);

            if (meta.dsFlag(DSFlag.REQUIRES_OBJLVL)) {
                for (ObjLvl objLvl : meta.objLvls()) {
                    if (!columns.containsKey(objLvl)) {
                        throw new InvalidConfigurationException("Transform " + tfVerb + " requires attribute level " + objLvl);
                    }
                }
            }
        } else {
            if (ctx.columns_item() != null) {
                columns = getColumns(ctx.columns_item(), StreamType.Passthru);
            }
        }

        List<Expressions.ExprItem<?>> keyExpression;
        String ke;
        TDL4.Key_itemContext keyExpr = ctx.key_item();
        if (keyExpr != null) {
            keyExpression = expression(keyExpr.expression().children, ExpressionRules.RECORD);
            ke = keyExpr.expression().getText();
        } else {
            keyExpression = Collections.emptyList();
            ke = null;
        }

        boolean repartition = false;
        int partCount = 0;
        if (ctx.K_PARTITION() != null) {
            repartition = true;

            if (ctx.expression() != null) {
                Object parts = Expressions.eval(null, null, expression(ctx.expression().children, ExpressionRules.LOOSE), variables);
                partCount = (parts instanceof Number) ? ((Number) parts).intValue() : Utils.parseNumber(String.valueOf(parts)).intValue();
                if (partCount < 1) {
                    throw new InvalidConfigurationException("ALTER \"" + dsNames + "\" requested number of PARTITIONs below 1");
                }
            }
        }

        int ut = DataContext.usageThreshold();
        for (String dsName : dataStreams) {
            if (verbose) {
                System.out.println("ALTERing DS " + dsName + ": " + dataContext.streamInfo(dsName).describe(ut));
            }
            StreamInfo si = dataContext.alterDataStream(dsName,
                    (meta != null) ? meta.verb : null, columns, params,
                    keyExpression, ke,
                    repartition, partCount,
                    variables);
            if (verbose) {
                System.out.println("ALTERed DS " + dsName + ": " + si.describe(ut));
            }
        }
    }

    private Map<ObjLvl, List<String>> getColumns(List<TDL4.Columns_itemContext> columnsItemContexts, StreamType requested) {
        Map<ObjLvl, List<String>> columns = new HashMap<>();

        for (TDL4.Columns_itemContext columnsItem : columnsItemContexts) {
            List<String> columnList;

            if (columnsItem.var_name() != null) {
                ArrayWrap namesArr = variables.getArray(resolveName(columnsItem.var_name().L_IDENTIFIER()));
                if (namesArr == null) {
                    throw new InvalidConfigurationException("SET <attribute_list_variable> references to NULL variable $" + columnsItem.var_name().L_IDENTIFIER());
                }

                columnList = Arrays.stream(namesArr.data()).map(String::valueOf).collect(Collectors.toList());
            } else {
                columnList = columnsItem.L_IDENTIFIER().stream().map(this::resolveName).collect(Collectors.toList());
            }

            ObjLvl columnsType = resolveObjLvl(columnsItem.T_OBJLVL());
            switch (requested) {
                case PlainText:
                case Columnar:
                case Structured: {
                    if (columnsType == ObjLvl.VALUE) {
                        columns.put(ObjLvl.VALUE, columnList);
                    }
                    break;
                }
                case Point: {
                    if (columnsType == ObjLvl.POINT) {
                        columns.put(ObjLvl.POINT, columnList);
                    }
                    break;
                }
                case Track: {
                    if (columnsType == ObjLvl.TRACK) {
                        columns.put(ObjLvl.TRACK, columnList);
                    } else if (columnsType == ObjLvl.POINT) {
                        columns.put(ObjLvl.POINT, columnList);
                    } else if (columnsType == ObjLvl.SEGMENT) {
                        columns.put(ObjLvl.SEGMENT, columnList);
                    }
                    break;
                }
                case Polygon: {
                    if (columnsType == ObjLvl.POLYGON) {
                        columns.put(ObjLvl.POLYGON, columnList);
                    }
                    break;
                }
                case Passthru: {
                    columns.put(columnsType, columnList);
                    break;
                }
            }
        }

        return columns;
    }

    private void copy(TDL4.Copy_stmtContext ctx) {
        TDL4.Func_exprContext funcExpr = ctx.func_expr();
        String outVerb = resolveName(funcExpr.func().L_IDENTIFIER());

        if (!Pluggables.OUTPUTS.containsKey(outVerb)) {
            throw new InvalidConfigurationException("Storage output adapter \"" + outVerb + "\" isn't present");
        }

        List<DataStream> dataStreams = ctx.from_scope().stream().map(this::fromScope).toList();

        PluggableMeta meta = Pluggables.OUTPUTS.get(outVerb).meta;
        List<StreamType> types = Arrays.asList(((InputMeta) meta.input).type.types);

        for (DataStream ds : dataStreams) {
            if (!types.contains(ds.streamType)) {
                throw new InvalidConfigurationException("Storage output adapter \"" + outVerb + "\" doesn't support DataStream \"" + ds.name + "\" of type " + ds.streamType);
            }
        }

        String path = String.valueOf(Expressions.eval(null, null, expression(ctx.expression().children, ExpressionRules.LOOSE), variables));

        Map<String, Object> params = resolveParams(funcExpr.params_expr());
        if (verbose) {
            System.out.println("COPY parameters: " + defParams(meta.definitions, params) + "\n");
        }
        checkMeta(outVerb, params, meta);

        int ut = DataContext.usageThreshold();
        for (DataStream dataStream : dataStreams) {
            if (verbose) {
                System.out.println("COPYing DS " + dataStream.name + ": " + dataContext.streamInfo(dataStream.name).describe(ut));
            }

            dataContext.copyDataStream(outVerb, dataStream, path, params, getColumns(ctx.columns_item(), dataStream.streamType));

            if (verbose) {
                System.out.println("Lineage:");
                for (StreamLineage sl : dataContext.get(dataStream.name).lineage) {
                    System.out.println("\t" + sl.toString());
                }
            }
        }
    }

    private int[] getParts(List<ParseTree> children, VariablesContext variables) {
        int[] partitions;

        Object parts = Expressions.eval(null, null, expression(children, ExpressionRules.LOOSE), variables);
        if (parts instanceof ArrayWrap) {
            partitions = Arrays.stream(((ArrayWrap) parts).data()).mapToInt(p -> Integer.parseInt(String.valueOf(p))).toArray();
        } else {
            partitions = new int[]{Integer.parseInt(String.valueOf(parts))};
        }

        return partitions;
    }

    private void let(TDL4.Let_stmtContext ctx) {
        String varName = resolveName(ctx.var_name().L_IDENTIFIER());

        if (CWD_VAR.equals(varName)) {
            return;
        }

        Object value = null;
        if (ctx.expression() != null) {
            value = Expressions.eval(null, null, expression(ctx.expression().children, ExpressionRules.PARAM), variables);
        }
        if (ctx.sub_query() != null) {
            value = subQuery(ctx.sub_query()).toArray();
        }

        variables.put(varName, value);

        if (verbose) {
            System.out.println("Variable $" + varName + ": " + variables.varInfo(varName).describe());
        }
    }

    private void loop(TDL4.Loop_stmtContext ctx) {
        String varName = resolveName(ctx.var_name().L_IDENTIFIER());

        Object expr = Expressions.eval(null, null, expression(ctx.expression().children, ExpressionRules.LOOSE), variables);
        boolean loop = expr != null;

        Object[] loopValues = null;
        if (loop) {
            loopValues = new ArrayWrap(expr).data();

            loop = loopValues.length > 0;
        }

        if (loop) {
            int loop_limit = options.getNumber(loop_iteration_limit.name(), loop_iteration_limit.def()).intValue();
            if (loop_limit < loopValues.length) {
                String msg = "LOOP iteration limit " + loop_limit + " is exceeded." +
                        " There are " + loopValues.length + " values to LOOP by control variable $" + varName;
                System.out.println(msg + " \n");

                throw new InvalidConfigurationException(msg);
            }

            int loop_nest = options.getNumber(loop_nesting_limit.name(), loop_nesting_limit.def()).intValue();
            if (variables.level > loop_nest) {
                String msg = "LOOP nesting limit " + loop_nest + " is exceeded by control variable $" + varName;
                System.out.println(msg + " \n");

                throw new InvalidConfigurationException(msg);
            }

            if (verbose) {
                System.out.println("LOOP control variable $" + varName + " values list: " + Arrays.toString(loopValues) + "\n");
            }

            variables = new VariablesContext(variables);

            for (Object val : loopValues) {
                if (verbose) {
                    System.out.println("LOOP iteration variable $" + varName + " value: " + val + "\n");
                }

                variables.put(varName, val);

                for (TDL4.StatementContext stmt : ctx.statements(0).statement()) {
                    statement(stmt);
                }
            }

            variables = variables.parent;
        } else {
            if (ctx.statements(1) != null) {
                if (verbose) {
                    System.out.println("LOOP ELSE branch\n");
                }

                for (TDL4.StatementContext stmt : ctx.statements(1).statement()) {
                    statement(stmt);
                }
            }
        }
    }

    private void ifElse(TDL4.If_stmtContext ctx) {
        TDL4.ExpressionContext expr = ctx.expression();

        boolean then = Expressions.bool(null, null, expression(expr.children, ExpressionRules.LOOSE), variables);
        if (then) {
            if (verbose) {
                System.out.println("IF THEN branch\n");
            }

            for (TDL4.StatementContext stmt : ctx.statements(0).statement()) {
                statement(stmt);
            }
        } else {
            if (ctx.statements(1) != null) {
                if (verbose) {
                    System.out.println("IF ELSE branch\n");
                }

                for (TDL4.StatementContext stmt : ctx.statements(1).statement()) {
                    statement(stmt);
                }
            }
        }
    }

    private List<ParseTree> doShuntingYard(List<ParseTree> exprChildren) {
        Deque<ParseTree> whereOpStack = new LinkedList<>();
        List<ParseTree> predExpStack = new ArrayList<>();
        int i = 0;
        // doing Shunting Yard
        for (; i < exprChildren.size(); i++) {
            ParseTree child = exprChildren.get(i);

            if ((child instanceof TDL4.Sym_opContext)
                    || (child instanceof TDL4.Kw_opContext)
                    || (child instanceof TDL4.In_opContext)
                    || (child instanceof TDL4.Is_opContext)
                    || (child instanceof TDL4.Between_opContext)) {
                while (!whereOpStack.isEmpty()) {
                    ParseTree peek = whereOpStack.peek();

                    if (isHigher(child, peek)) {
                        predExpStack.add(whereOpStack.pop());
                    } else {
                        break;
                    }
                }

                whereOpStack.push(child);
                continue;
            }

            if (child instanceof TDL4.Func_callContext funcCall) {
                if (funcCall.expression() != null) {
                    for (TDL4.ExpressionContext e : funcCall.expression()) {
                        predExpStack.addAll(doShuntingYard(e.children));
                    }
                }

                if (funcCall.func() != null) {
                    predExpStack.add(funcCall);
                }
                continue;
            }

            // expression
            predExpStack.add(child);
        }

        while (!whereOpStack.isEmpty()) {
            predExpStack.add(whereOpStack.pop());
        }

        return predExpStack;
    }

    private List<Expressions.ExprItem<?>> expression(List<ParseTree> exprChildren, ExpressionRules rules) {
        List<Expressions.ExprItem<?>> items = new ArrayList<>();

        List<ParseTree> predExpStack = doShuntingYard(exprChildren);

        for (ParseTree exprItem : predExpStack) {
            if (exprItem instanceof TDL4.AttrContext) {
                switch (rules) {
                    case RECORD: {
                        String propName = resolveName(((TDL4.AttrContext) exprItem).L_IDENTIFIER());

                        items.add(Expressions.attrItem(propName));
                        continue;
                    }
                    case PARAM: {
                        String propName = resolveName(((TDL4.AttrContext) exprItem).L_IDENTIFIER());

                        items.add(Expressions.stringItem(propName));
                        continue;
                    }
                    default: {
                        throw new InvalidConfigurationException("Attribute name is not allowed in this context: " + exprItem.getText());
                    }
                }
            }

            if (exprItem instanceof TDL4.Var_nameContext varNameCtx) {

                String varName = resolveName(varNameCtx.L_IDENTIFIER());

                items.add(Expressions.varItem(varName));
                continue;
            }

            // NOT? BETWEEN
            if (exprItem instanceof TDL4.Between_opContext between) {
                items.add(Expressions.stackGetter(1));

                double l = resolveNumericLiteral(between.L_NUMERIC(0)).doubleValue();
                double r = resolveNumericLiteral(between.L_NUMERIC(1)).doubleValue();
                items.add((between.S_NOT() == null)
                        ? Expressions.between(l, r)
                        : Expressions.notBetween(l, r)
                );

                continue;
            }

            // NOT? IN
            if (exprItem instanceof TDL4.In_opContext) {
                items.add(Expressions.stackGetter(2));

                items.add(((TDL4.In_opContext) exprItem).S_NOT() != null ? Expressions.notIn() : Expressions.in());

                continue;
            }

            // IS NOT? NULL
            if (exprItem instanceof TDL4.Is_opContext) {
                items.add(Expressions.stackGetter(1));

                items.add((((TDL4.Is_opContext) exprItem).S_NOT() == null) ? Expressions.isNull() : Expressions.isNotNull());

                continue;
            }

            if ((exprItem instanceof TDL4.Sym_opContext)
                    || (exprItem instanceof TDL4.Kw_opContext)) {
                OperatorInfo oi = Operators.OPERATORS.get(exprItem.getText());
                if (oi == null) {
                    throw new RuntimeException("Unknown operator token " + exprItem.getText());
                } else {
                    int arity = oi.arity;
                    items.add(Expressions.stackGetter(arity));
                    items.add(Expressions.opItem(oi.instance));
                }

                continue;
            }

            if (exprItem instanceof TDL4.ArrayContext array) {
                Object[] values = null;
                if (array.S_RANGE() != null) {
                    Number a = resolveNumericLiteral(array.L_NUMERIC(0));
                    Number b = resolveNumericLiteral(array.L_NUMERIC(1));

                    values = ArrayFunctions.MakeRange.getRange(a, b);
                } else {
                    if (!array.L_IDENTIFIER().isEmpty()) {
                        if (rules != ExpressionRules.PARAM) {
                            throw new InvalidConfigurationException("Attribute name is not allowed in this context: " + exprItem.getText());
                        }
                        values = array.L_IDENTIFIER().stream()
                                .map(this::resolveName)
                                .toArray(String[]::new);
                    }
                    if (!array.literal().isEmpty()) {
                        values = array.literal().stream()
                                .map(this::resolveLiteral)
                                .toArray(Object[]::new);
                    }
                }

                items.add(Expressions.arrayItem(new ArrayWrap(values)));

                continue;
            }

            if (exprItem instanceof TDL4.Func_callContext funcCall) {
                TDL4.FuncContext funcCtx = funcCall.func();
                String funcName = resolveName(funcCtx.L_IDENTIFIER());

                FunctionInfo fi = Functions.FUNCTIONS.get(funcName);
                if (fi == null) {
                    if (library.functions.containsKey(funcName)) {
                        fi = library.functions.get(funcName);
                    } else {
                        throw new RuntimeException("Unknown function token " + exprItem.getText());
                    }
                }

                int arity = fi.arity;

                if ((arity < Function.ARBITR_ARY) && (rules != ExpressionRules.RECORD)) {
                    throw new RuntimeException("Record-related function " + fi.symbol + " can't be called outside of query context");
                }

                switch (arity) {
                    case Function.RECORD_KEY: {
                        items.add(Expressions.keyItem(funcCall.expression().size()));
                        break;
                    }
                    case Function.RECORD_OBJECT: {
                        items.add(Expressions.objItem(funcCall.expression().size()));
                        break;
                    }
                    case Function.WHOLE_RECORD: {
                        items.add(Expressions.recItem(funcCall.expression().size()));
                        break;
                    }
                    case Function.RECORD_LEVEL:
                    case Function.ARBITR_ARY: {
                        items.add(Expressions.stackGetter(funcCall.expression().size()));
                        break;
                    }
                    case Function.NO_ARGS: {
                        break;
                    }
                    default: {
                        items.add(Expressions.stackGetter(arity));
                    }
                }
                items.add(Expressions.funcItem(fi.instance));

                continue;
            }

            if (exprItem instanceof TDL4.LiteralContext literal) {
                if (literal.L_STRING() != null) {
                    items.add(Expressions.stringItem(resolveStringLiteral(literal.L_STRING())));
                    continue;
                }
                if (literal.L_NUMERIC() != null) {
                    items.add(Expressions.numericItem(resolveNumericLiteral(literal.L_NUMERIC())));
                    continue;
                }
                if (literal.S_TRUE() != null) {
                    items.add(Expressions.boolItem(true));
                    continue;
                }
                if (literal.S_FALSE() != null) {
                    items.add(Expressions.boolItem(false));
                    continue;
                }
                items.add(Expressions.nullItem());

                continue;
            }
        }

        return items;
    }

    private void select(TDL4.Select_stmtContext ctx) {
        List<TDL4.Select_ioContext> selectIO = ctx.select_io();

        List<TDL4.Select_ioContext> fromIO = selectIO.stream().filter(fs -> fs.K_FROM() != null).toList();
        if (fromIO.isEmpty()) {
            throw new RuntimeException("SELECT without FROM");
        }

        int scopes = fromIO.size();

        List<TDL4.Select_ioContext> intoIO = selectIO.stream().filter(sio -> sio.K_INTO() != null).toList();
        if (intoIO.size() != scopes) {
            throw new RuntimeException("INTO list in SELECT must have same size as FROM list");
        }

        List<String> intoNames = new ArrayList<>();
        List<DataStream> sources = new ArrayList<>();
        for (int i = 0; i < scopes; i++) {
            TDL4.Select_ioContext from = fromIO.get(i);
            TDL4.Select_ioContext into = intoIO.get(i);

            String intoName = resolveName(into.ds_name().L_IDENTIFIER());
            if (from.from_wildcard() != null) {
                if (into.S_STAR() == null) {
                    throw new RuntimeException("Each FROM with wildcard in SELECT must have matching INTO with wildcard");
                }

                List<DataStream> dsList = fromWildcard(from.from_wildcard());
                sources.addAll(dsList);

                int prefixLength = resolveName(from.ds_name().L_IDENTIFIER()).length();
                dsList.forEach(ds -> intoNames.add(intoName + ds.name.substring(prefixLength)));
            } else {
                sources.add(fromScope(from.from_scope()));

                intoNames.add(intoName);
            }
        }

        for (String intoName : intoNames) {
            if (dataContext.has(intoName)) {
                throw new InvalidConfigurationException("SELECT INTO \"" + intoName + "\" tries to create DataStream \"" + intoName + "\" which already exists");
            }
        }

        boolean distinct = ctx.K_DISTINCT() != null;

        Long limitRecords = null;
        Double limitPercent = null;

        if (ctx.limit_expr() != null) {
            String limit = String.valueOf(Expressions.eval(null, null, expression(ctx.limit_expr().expression().children, ExpressionRules.LOOSE), variables));

            if (ctx.limit_expr().S_PERCENT() != null) {
                limitPercent = Double.parseDouble(limit);
                if ((limitPercent <= 0) || (limitPercent > 100)) {
                    throw new RuntimeException("Percentage in LIMIT clause can't be 0 or less and more than 100");
                }
            } else {
                limitRecords = Long.parseLong(limit);
                if (limitRecords <= 0) {
                    throw new RuntimeException("Record number in LIMIT clause can't be 0 or less");
                }
            }
        }

        WhereItem whereItem = new WhereItem();
        TDL4.Where_exprContext whereCtx = ctx.where_expr();
        if (whereCtx != null) {
            ObjLvl category = resolveObjLvl(whereCtx.T_OBJLVL());
            whereItem = new WhereItem(expression(whereCtx.expression().children, ExpressionRules.RECORD), category);
        }

        boolean star = (ctx.S_STAR() != null);

        List<SelectItem> items;
        if (star) {
            items = Collections.emptyList();
        } else {
            items = new ArrayList<>();
            for (TDL4.What_exprContext expr : ctx.what_expr()) {
                List<ParseTree> exprTree = expr.expression().children;
                List<Expressions.ExprItem<?>> selectItem = expression(exprTree, ExpressionRules.RECORD);

                String alias;
                TDL4.AliasContext aliasCtx = expr.alias();
                if (aliasCtx != null) {
                    alias = resolveName(aliasCtx.L_IDENTIFIER());
                } else {
                    if ((exprTree.size() == 1) && (exprTree.get(0) instanceof TDL4.AttrContext)) {
                        alias = resolveName(((TDL4.AttrContext) exprTree.get(0)).L_IDENTIFIER());
                    } else {
                        alias = expr.expression().getText();
                    }
                }

                ObjLvl typeAlias = resolveObjLvl(expr.T_OBJLVL());
                items.add(new SelectItem(selectItem, alias, typeAlias));
            }
        }

        for (int i = 0; i < scopes; i++) {
            String intoName = intoNames.get(i);

            DataStream resultDs = dataContext.select(sources.get(i), intoName, distinct, star, items, whereItem, limitRecords, limitPercent, variables);
            dataContext.put(intoName, resultDs);

            if (verbose) {
                JavaPairRDD<Object, DataRecord<?>> rdd = dataContext.rdd(resultDs);
                System.out.println("SELECTing INTO DS " + intoName + ": " + new StreamInfo(resultDs.attributes(), resultDs.keyExpr, rdd.getStorageLevel().description(),
                        resultDs.streamType.name(), rdd.getNumPartitions(), resultDs.getUsages()).describe(DataContext.usageThreshold()));
            }
        }
    }

    private List<DataStream> fromWildcard(TDL4.From_wildcardContext ctx) {
        ListOrderedMap<String, int[]> fromParts = new ListOrderedMap<>();

        List<String> names = dataContext.getStarNames(resolveName(ctx.ds_name().L_IDENTIFIER()));

        int[] parts = (ctx.ds_parts() != null) ? getParts(ctx.ds_parts().expression().children, variables) : null;
        for (String name : names) {
            fromParts.put(name, parts);
        }

        return fromParts.entrySet().stream().map(dsp -> dataContext.partition(dsp.getKey(), dsp.getValue())).toList();
    }

    private DataStream fromScope(TDL4.From_scopeContext fromScope) {
        ListOrderedMap<String, int[]> fromParts = new ListOrderedMap<>();

        if (fromScope.S_STAR() == null) {
            for (TDL4.Ds_name_partsContext e : fromScope.ds_name_parts()) {
                fromParts.put(resolveName(e.L_IDENTIFIER()), (e.K_PARTITION() != null) ? getParts(e.expression().children, variables) : null);
            }
        }

        if ((fromScope.union_op() == null) && (fromScope.join_op() == null)) {
            return dataContext.partition(fromParts.get(0), fromParts.getValue(0));
        }

        if (fromScope.join_op() != null) {
            return dataContext.fromJoin(fromParts, JoinSpec.get(fromScope.join_op().getText()));
        }

        String prefix = null;
        if (fromScope.S_STAR() != null) {
            prefix = resolveName(fromScope.ds_name().L_IDENTIFIER());
            List<String> names = dataContext.getStarNames(prefix);

            int[] parts = (fromScope.ds_parts() != null) ? getParts(fromScope.ds_parts().expression().children, variables) : null;
            for (String name : names) {
                fromParts.put(name, parts);
            }
        }

        return dataContext.fromUnion(prefix, fromParts, UnionSpec.get(fromScope.union_op().getText()));
    }

    private Collection<?> subQuery(TDL4.Sub_queryContext ctx) {
        TDL4.From_scopeContext fromScope = ctx.from_scope();

        if ((fromScope.S_STAR() != null) && (fromScope.union_op() == null)) {
            throw new RuntimeException("Subqueries don't support multiple sources in FROM list");
        }

        TDL4.What_exprContext expr = ctx.what_expr();

        List<ParseTree> exprTree = expr.expression().children;
        List<Expressions.ExprItem<?>> selectItem = expression(exprTree, ExpressionRules.RECORD);

        Long limitRecords = null;
        Double limitPercent = null;

        if (ctx.limit_expr() != null) {
            String limit = String.valueOf(Expressions.eval(null, null, expression(ctx.limit_expr().expression().children, ExpressionRules.LOOSE), variables));

            if (ctx.limit_expr().S_PERCENT() != null) {
                limitPercent = Double.parseDouble(limit);
                if ((limitPercent <= 0) || (limitPercent > 100)) {
                    throw new RuntimeException("Percentage in LIMIT clause can't be 0 or less and more than 100");
                }
            } else {
                limitRecords = Long.parseLong(limit);
                if (limitRecords <= 0) {
                    throw new RuntimeException("Record number in LIMIT clause can't be 0 or less");
                }
            }
        }

        List<Expressions.ExprItem<?>> whereItem = null;
        TDL4.Where_exprContext whereCtx = ctx.where_expr();
        if (whereCtx != null) {
            whereItem = expression(whereCtx.expression().children, ExpressionRules.RECORD);
        }

        boolean distinct = ctx.K_DISTINCT() != null;

        return dataContext.subQuery(fromScope(fromScope), distinct, selectItem, whereItem, limitRecords, limitPercent, variables);
    }

    private void call(TDL4.Call_stmtContext ctx) {
        TDL4.Func_exprContext funcExpr = ctx.func_expr();

        String verb = resolveName(funcExpr.func().L_IDENTIFIER());
        Map<String, Object> params = resolveParams(funcExpr.params_expr());
        if (ctx.operation_io() != null) {
            if (!Pluggables.OPERATIONS.containsKey(verb)) {
                throw new InvalidConfigurationException("CALL \"" + verb + "\"() refers to unknown Operation or Transform");
            }

            callOperation(verb, params, ctx.operation_io());
        } else {
            if (!library.procedures.containsKey(verb)) {
                throw new InvalidConfigurationException("CALL \"" + verb + "\"() refers to undefined PROCEDURE");
            }

            callProcedure(verb, params);
        }
    }

    private void callOperation(String opVerb, Map<String, Object> params, List<TDL4.Operation_ioContext> ctx) {
        PluggableInfo pi = Pluggables.OPERATIONS.get(opVerb);

        PluggableMeta meta = pi.meta;
        if (verbose) {
            System.out.println("CALL parameters: " + defParams(meta.definitions, params) + "\n");
        }
        checkMeta(opVerb, params, meta);

        List<ListOrderedMap<String, DataStream>> inputMaps = new ArrayList<>();
        Map<Integer, Integer> wildcards = new HashMap<>();
        boolean namedInput = meta.input instanceof NamedInputMeta;
        if (!namedInput) {
            List<TDL4.Operation_ioContext> inputIO = ctx.stream().filter(c -> (c.input_anonymous() != null) || (c.input_wildcard() != null)).toList();

            if (inputIO.isEmpty() || ctx.stream().anyMatch(c -> c.input_named() != null)) {
                throw new InvalidConfigurationException("CALL " + opVerb + "() requires anonymous or wildcard INPUT specification");
            }

            for (int i = 0; i < inputIO.size(); i++) {
                TDL4.Operation_ioContext input = inputIO.get(i);

                ListOrderedMap<String, DataStream> inputMap = new ListOrderedMap<>();
                if (input.input_wildcard() != null) {
                    TDL4.From_wildcardContext fromScope = input.input_wildcard().from_wildcard();

                    String prefix = resolveName(fromScope.ds_name().L_IDENTIFIER());
                    wildcards.put(i, prefix.length());

                    fromWildcard(fromScope).forEach(ds -> inputMap.put(ds.name, ds));
                } else {
                    List<TDL4.From_scopeContext> fromScopes = input.input_anonymous().from_scope();

                    for (TDL4.From_scopeContext fromScope : fromScopes) {
                        DataStream source = fromScope(fromScope);

                        inputMap.put(source.name, source);
                    }
                }
                inputMaps.add(inputMap);

                List<StreamType> types = Arrays.asList(((InputMeta) meta.input).type.types);
                for (Map.Entry<String, DataStream> inputDs : inputMap.entrySet()) {
                    if (!types.contains(inputDs.getValue().streamType)) {
                        throw new InvalidConfigurationException("CALL " + opVerb + "() doesn't accept INPUT from anonymous DataStream \""
                                + inputDs.getKey() + "\" of type " + inputDs.getValue().streamType);
                    }
                }
            }
        } else {
            NamedInputMeta nsm = (NamedInputMeta) meta.input;

            List<TDL4.Operation_ioContext> inputScopes = ctx.stream().filter(c -> c.input_named() != null).toList();
            if (inputScopes.isEmpty() || ctx.stream().anyMatch(c -> c.input_anonymous() != null) || ctx.stream().anyMatch(c -> c.input_wildcard() != null)) {
                throw new InvalidConfigurationException("CALL " + opVerb + "() requires aliased INPUT specification");
            }

            for (TDL4.Operation_ioContext inputScope : inputScopes) {
                TDL4.Input_namedContext inputNamed = inputScope.input_named();

                List<TDL4.From_scopeContext> fromScopes = inputNamed.from_scope();

                ListOrderedMap<String, DataStream> inputMap = new ListOrderedMap<>();
                for (int i = 0; i < fromScopes.size(); i++) {
                    TDL4.From_scopeContext fromScope = fromScopes.get(i);

                    inputMap.put(resolveName(inputNamed.ds_alias(i).L_IDENTIFIER()), fromScope(fromScope));
                }

                for (Map.Entry<String, InputMeta> ns : nsm.streams.entrySet()) {
                    String alias = ns.getKey();
                    InputMeta dsm = ns.getValue();

                    if (!dsm.optional && !inputMap.containsKey(alias)) {
                        throw new InvalidConfigurationException("CALL " + opVerb + "() requires aliased INPUT " + alias + ", but it wasn't supplied");
                    }

                    if (inputMap.containsKey(alias)) {
                        DataStream source = inputMap.get(alias);
                        if (!Arrays.asList(nsm.streams.get(alias).type.types).contains(source.streamType)) {
                            throw new InvalidConfigurationException("CALL " + opVerb + "() doesn't accept aliased INPUT " + alias + " FROM  DataStream \""
                                    + source.name + "\" of type " + source.streamType);
                        }
                    }
                }

                inputMaps.add(inputMap);
            }
        }

        int ioSize = inputMaps.size();
        List<ListOrderedMap<String, String>> outputMaps = new ArrayList<>();

        boolean namedOutput = meta.output instanceof NamedOutputMeta;
        if (!namedOutput) {
            List<TDL4.Operation_ioContext> outputIO = ctx.stream().filter(c -> (c.output_anonymous() != null) || (c.output_wildcard() != null)).toList();
            if ((outputIO.size() != ioSize) || ctx.stream().anyMatch(c -> c.output_named() != null)) {
                throw new InvalidConfigurationException("CALL " + opVerb + "() requires same amount of anonymous OUTPUT specifications as INPUT");
            }

            for (int i = 0; i < ioSize; i++) {
                TDL4.Operation_ioContext intoAnon = outputIO.get(i);

                ListOrderedMap<String, String> outputMap = new ListOrderedMap<>();
                if (wildcards.get(i) != null) {
                    if (intoAnon.output_wildcard() == null) {
                        throw new InvalidConfigurationException("CALL " + opVerb + "() requires matching wildcard OUTPUT specification for each wildcard INPUT");
                    }

                    String outputPrefix = resolveName(intoAnon.output_wildcard().ds_name().L_IDENTIFIER());
                    int prefixLen = wildcards.get(i);
                    inputMaps.get(i).keyList().stream().map(dsn -> outputPrefix + dsn.substring(prefixLen)).forEach(dsn -> outputMap.put(dsn, dsn));
                } else {
                    if (intoAnon.output_anonymous() == null) {
                        throw new InvalidConfigurationException("CALL " + opVerb + "() requires matching anonymous OUTPUT specification for each anonymous INPUT");
                    }

                    intoAnon.output_anonymous().ds_name().stream().map(dsn -> resolveName(dsn.L_IDENTIFIER())).forEach(dsn -> outputMap.put(dsn, dsn));
                }

                for (String outputName : outputMap.values()) {
                    if (dataContext.has(outputName)) {
                        throw new InvalidConfigurationException("CALL " + opVerb + "() OUTPUT tries to create DataStream \"" + outputName + "\" which already exists");
                    }
                }

                outputMaps.add(outputMap);
            }
        } else {
            NamedOutputMeta nsm = (NamedOutputMeta) meta.output;

            List<TDL4.Output_namedContext> intoCtx = ctx.stream().map(TDL4.Operation_ioContext::output_named).filter(Objects::nonNull).toList();
            if ((intoCtx.size() != ioSize) || ctx.stream().anyMatch(c -> c.output_anonymous() != null) || ctx.stream().anyMatch(c -> c.output_wildcard() != null)) {
                throw new InvalidConfigurationException("CALL " + opVerb + "() requires same amount of aliased OUTPUT specifications as INPUT");
            }

            for (int i = 0; i < ioSize; i++) {
                TDL4.Output_namedContext intoNamed = intoCtx.get(i);

                List<TDL4.Ds_nameContext> dsNames = intoNamed.ds_name();
                List<TDL4.Ds_aliasContext> dsAliases = intoNamed.ds_alias();
                ListOrderedMap<String, String> outputMap = new ListOrderedMap<>();
                for (int j = 0; j < dsNames.size(); j++) {
                    outputMap.put(resolveName(dsAliases.get(j).L_IDENTIFIER()), resolveName(dsNames.get(j).L_IDENTIFIER()));
                }

                for (Map.Entry<String, String> outputName : outputMap.entrySet()) {
                    if (dataContext.has(outputName.getValue())) {
                        throw new InvalidConfigurationException("CALL " + opVerb + "() OUTPUT " + outputName.getKey() + " INTO tries to create DataStream \"" + outputName.getValue() + "\" which already exists");
                    }
                }

                for (Map.Entry<String, OutputMeta> ns : nsm.streams.entrySet()) {
                    OutputMeta dsm = ns.getValue();

                    String dsAlias = ns.getKey();
                    if (!dsm.optional && !outputMap.containsKey(dsAlias)) {
                        throw new InvalidConfigurationException("CALL " + opVerb + "() OUTPUT " + dsAlias + " INTO requires DataStream reference, but it wasn't supplied");
                    }
                }

                outputMaps.add(outputMap);
            }
        }

        int ut = DataContext.usageThreshold();

        try {
            Pluggable op = pi.newInstance();
            op.configure(new Configuration(meta.definitions, opVerb, params));

            for (int i = 0; i < ioSize; i++) {
                ListOrderedMap<String, DataStream> inputMap = inputMaps.get(i);
                ListOrderedMap<String, String> outputMap = outputMaps.get(i);

                if (verbose) {
                    for (Map.Entry<String, DataStream> inpName : inputMap.entrySet()) {
                        System.out.println("CALL INPUT DS " + inpName.getKey() + ": " + dataContext.streamInfo(inpName.getValue().name).describe(ut));
                    }
                }

                InputOutput input = namedInput
                        ? new NamedInput(inputMap)
                        : new Input(inputMap.getValue(0));

                InputOutput output = namedOutput
                        ? new NamedOutput(outputMap)
                        : new Output(outputMap.getValue(0), null);

                op.initialize(input, output);
                op.execute();

                Map<String, DataStream> result = op.result();
                for (DataStream ds : result.values()) {
                    dataContext.put(ds.name, ds);

                    if (verbose) {
                        System.out.println("CALL OUTPUT DS " + ds.name + ": " + dataContext.streamInfo(ds.name).describe(ut));
                    }
                }
            }
        } catch (Exception e) {
            throw new InvalidConfigurationException("CALL " + opVerb + "() failed with an exception", e);
        }
    }

    private void checkMeta(String verb, Map<String, Object> params, PluggableMeta meta) {
        if (meta.definitions != null) {
            for (Map.Entry<String, DefinitionMeta> defEntry : meta.definitions.entrySet()) {
                String name = defEntry.getKey();
                DefinitionMeta def = defEntry.getValue();

                if (!def.optional && !params.containsKey(name)) {
                    throw new InvalidConfigurationException(verb + "\"() must have mandatory parameter @" + name + " set");
                }
            }
        }
    }

    private void callProcedure(String procName, Map<String, Object> params) {
        if (verbose) {
            System.out.println("CALLing PROCEDURE " + procName + " with params " + params + "\n");
        }

        Procedure proc = library.procedures.get(procName);

        for (Map.Entry<String, Param> defEntry : proc.params.entrySet()) {
            String name = defEntry.getKey();
            Param paramDef = defEntry.getValue();

            if (!paramDef.optional && !params.containsKey(name)) {
                throw new InvalidConfigurationException("PROCEDURE " + procName + " CALL must have mandatory parameter @" + name + " set");
            }
        }

        variables = new VariablesContext(variables);
        variables.putAll(params);

        for (TDL4.StatementContext stmt : proc.ctx.statement()) {
            statement(stmt);
        }

        variables = variables.parent;
    }

    private void analyze(TDL4.Analyze_stmtContext ctx) {
        TDL4.Key_itemContext keyExpr = ctx.key_item();
        List<Expressions.ExprItem<?>> keyExpression;
        String ke;
        if (keyExpr != null) {
            keyExpression = expression(keyExpr.expression().children, ExpressionRules.RECORD);
            ke = keyExpr.expression().getText();
        } else {
            keyExpression = Collections.emptyList();
            ke = null;
        }

        String dsName = resolveName(ctx.ds_name().L_IDENTIFIER());

        ListOrderedMap<String, DataStream> dataStreams;
        if (ctx.S_STAR() != null) {
            dataStreams = dataContext.getAll(dsName, null);
        } else {
            dataStreams = new ListOrderedMap<>();
            dataStreams.put(dsName, dataContext.get(dsName));
        }

        int ut = DataContext.usageThreshold();
        if (verbose) {
            for (String dataStream : dataStreams.keyList()) {
                System.out.println("ANALYZEd DS " + dataStream + ": " + dataContext.streamInfo(dataStream).describe(ut));
                System.out.println("Lineage:");
                for (StreamLineage sl : dataContext.get(dataStream).lineage) {
                    System.out.println("\t" + sl.toString());
                }
                System.out.println();
            }
        }

        dataContext.analyze(dataStreams, keyExpression, ke, ctx.K_PARTITION() != null, variables);
    }

    private void createProcedure(TDL4.Create_procContext ctx) {
        String procName = resolveName(ctx.func().L_IDENTIFIER());

        if (Pluggables.OPERATIONS.containsKey(procName)) {
            throw new InvalidConfigurationException("Attempt to CREATE PROCEDURE which overrides OPERATION \"" + procName + "\"");
        }

        if ((ctx.K_REPLACE() == null) && library.procedures.containsKey(procName)) {
            throw new InvalidConfigurationException("PROCEDURE " + procName + " has already been defined. Offending definition at line " + ctx.K_CREATE().getSymbol().getLine());
        }

        Procedure.Builder proc = Procedure.builder(ctx.statements());
        buildParams(ctx.proc_param(), proc);

        library.procedures.put(procName, proc.build());
    }

    private void createFunction(TDL4.Create_funcContext ctx) {
        String funcName = resolveName(ctx.func().L_IDENTIFIER());

        if (Functions.FUNCTIONS.containsKey(funcName)) {
            throw new InvalidConfigurationException("Attempt to CREATE FUNCTION which overrides pluggable \"" + funcName + "\"");
        }

        if ((ctx.K_REPLACE() == null) && library.functions.containsKey(funcName)) {
            throw new InvalidConfigurationException("FUNCTION " + funcName + " has already been defined. Offending definition at line " + ctx.K_CREATE().getSymbol().getLine());
        }

        boolean recordLevel = ctx.K_RECORD() != null;
        List<Function.StatementItem> items;
        if (ctx.K_BEGIN() == null) {
            items = List.of(Function.funcReturn(expression(ctx.expression().children, recordLevel ? ExpressionRules.RECORD : ExpressionRules.LOOSE)));
        } else {
            items = funcStatements(ctx.func_stmts().func_stmt(), recordLevel ? ExpressionRules.RECORD : ExpressionRules.LOOSE);
        }

        Function.Builder func = Function.builder(funcName, items, variables);
        buildParams(ctx.proc_param(), func);

        library.functions.put(funcName, recordLevel ? func.recordLevel() : func.loose());
    }

    private void buildParams(List<TDL4.Proc_paramContext> paramsCtx, ParamsBuilder<?> builder) {
        for (TDL4.Proc_paramContext paramCtx : paramsCtx) {
            TDL4.ParamContext param = paramCtx.param();
            if (param == null) {
                builder.mandatory(resolveName(paramCtx.L_IDENTIFIER()));
            } else {
                builder.optional(resolveName(param.L_IDENTIFIER()), Expressions.eval(null, null, expression(param.expression().children, ExpressionRules.PARAM), variables));
            }
        }
    }

    private List<Function.StatementItem> funcStatements(List<TDL4.Func_stmtContext> stmts, ExpressionRules rules) {
        List<Function.StatementItem> items = new ArrayList<>();

        for (TDL4.Func_stmtContext funcStmt : stmts) {
            if (funcStmt.let_func() != null) {
                items.add(Function.funcLet(resolveName(funcStmt.let_func().var_name().L_IDENTIFIER()),
                        expression(funcStmt.let_func().expression().children, rules)
                ));
            }
            if (funcStmt.if_func() != null) {
                items.add(Function.funcIf(expression(funcStmt.if_func().expression().children, rules),
                        funcStatements(funcStmt.if_func().func_stmts(0).func_stmt(), rules),
                        (funcStmt.if_func().func_stmts(1) != null)
                                ? funcStatements(funcStmt.if_func().func_stmts(1).func_stmt(), rules)
                                : null
                ));
            }
            if (funcStmt.loop_func() != null) {
                items.add(Function.funcLoop(resolveName(funcStmt.loop_func().var_name().L_IDENTIFIER()),
                        expression(funcStmt.loop_func().expression().children, rules),
                        funcStatements(funcStmt.loop_func().func_stmts(0).func_stmt(), rules),
                        (funcStmt.loop_func().func_stmts(1) != null)
                                ? funcStatements(funcStmt.loop_func().func_stmts(1).func_stmt(), rules)
                                : null
                ));
            }
            if (funcStmt.return_func() != null) {
                items.add(Function.funcReturn(expression(funcStmt.return_func().expression().children, rules)));
            }
            if (funcStmt.raise_stmt() != null) {
                String lvl = (funcStmt.raise_stmt().T_MSGLVL() != null) ? funcStmt.raise_stmt().T_MSGLVL().getText() : null;
                items.add(Function.raise(lvl, expression(funcStmt.raise_stmt().expression().children, rules)));
            }
        }

        return items;
    }

    private void drop(TDL4.Drop_stmtContext ctx) {
        for (TDL4.FuncContext func : ctx.func()) {
            String name = resolveName(func.L_IDENTIFIER());

            if (ctx.K_FUNCTION() != null) {
                library.functions.remove(name);
            }
            if (ctx.K_PROCEDURE() != null) {
                library.procedures.remove(name);
            }
        }
    }

    private void raise(TDL4.Raise_stmtContext raiseCtx) {
        Object msg = Expressions.eval(null, null, expression(raiseCtx.expression().children, ExpressionRules.LOOSE), variables);

        MsgLvl lvl = (raiseCtx.T_MSGLVL() != null) ? MsgLvl.get(raiseCtx.T_MSGLVL().getText()) : MsgLvl.ERROR;
        switch (lvl) {
            case INFO -> System.out.println(msg);
            case WARNING -> System.err.println(msg);
            case ERROR -> throw new RaiseException(String.valueOf(msg));
        }
    }

    private Map<String, Object> resolveParams(TDL4.Params_exprContext params) {
        Map<String, Object> ret = new HashMap<>();

        if (params != null) {
            for (TDL4.ParamContext atRule : params.param()) {
                Object obj = Expressions.eval(null, null, expression(atRule.expression().children, ExpressionRules.PARAM), variables);

                ret.put(resolveName(atRule.L_IDENTIFIER()), obj);
            }
        }

        return ret;
    }

    private boolean isHigher(ParseTree o1, ParseTree o2) {
        OperatorInfo first = Operators.OPERATORS.get(o1.getText());
        if (o1 instanceof TDL4.In_opContext) {
            first = Operators.IN;
        }
        if (o1 instanceof TDL4.Is_opContext) {
            first = Operators.IS;
        }
        if (o1 instanceof TDL4.Between_opContext) {
            first = Operators.BETWEEN;
        }

        OperatorInfo second = Operators.OPERATORS.get(o2.getText());
        if (o2 instanceof TDL4.In_opContext) {
            second = Operators.IN;
        }
        if (o2 instanceof TDL4.Is_opContext) {
            second = Operators.IS;
        }
        if (o2 instanceof TDL4.Between_opContext) {
            second = Operators.BETWEEN;
        }

        return ((second.priority - first.priority) > 0) || ((first.priority == second.priority) && !first.rightAssoc);
    }

    private Number resolveNumericLiteral(TerminalNode numericLiteral) {
        if (numericLiteral != null) {
            return Utils.parseNumber(numericLiteral.getText());
        }
        return null;
    }

    private String resolveStringLiteral(TerminalNode stringLiteral) {
        if (stringLiteral != null) {
            String string = stringLiteral.getText();
            // SQL quoting character : '
            if ((string.charAt(0) == '\'') && (string.charAt(string.length() - 1) == '\'')) {
                string = string.substring(1, string.length() - 1);
            }
            string = string.replace("''", "'");

            return interpretString(string);
        }
        return null;
    }

    private String resolveName(TerminalNode identifier) {
        if (identifier != null) {
            String string = identifier.getText();
            // SQL quoting character : "
            if ((string.charAt(0) == '"') && (string.charAt(string.length() - 1) == '"')) {
                string = string.substring(1, string.length() - 1);
            }
            string = string.replace("\"\"", "\"");

            return interpretString(string);
        }
        return null;
    }

    private Object resolveLiteral(TDL4.LiteralContext l) {
        if (l.L_STRING() != null) {
            return resolveStringLiteral(l.L_STRING());
        }
        if (l.L_NUMERIC() != null) {
            return resolveNumericLiteral(l.L_NUMERIC());
        }
        if (l.S_TRUE() != null) {
            return true;
        }
        if (l.S_FALSE() != null) {
            return false;
        }
        return null;

    }

    private ObjLvl resolveObjLvl(TerminalNode typeAlias) {
        if (typeAlias != null) {
            return ObjLvl.get(typeAlias.getText());
        }
        return ObjLvl.VALUE;
    }

    private enum ExpressionRules {
        LOOSE, PARAM, RECORD
    }
}
