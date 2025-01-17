/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.scripting;

import io.github.pastorgl.datacooker.Options;
import io.github.pastorgl.datacooker.config.Configuration;
import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.data.*;
import io.github.pastorgl.datacooker.metadata.*;
import io.github.pastorgl.datacooker.storage.Adapters;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.commons.collections4.map.ListOrderedMap;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Function1;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static io.github.pastorgl.datacooker.Constants.CWD_VAR;
import static io.github.pastorgl.datacooker.Constants.STAR;
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

        return Expressions.evalLoose(expression(exprContext.expression().children, ExpressionRules.LET), variables);
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
        if (stmt.create_proc() != null) {
            createProcedure(stmt.create_proc());
        }
        if (stmt.drop_proc() != null) {
            dropProcedure(stmt.drop_proc());
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

        if (!Adapters.INPUTS.containsKey(inVerb)) {
            throw new InvalidConfigurationException("Storage input adapter \"" + inVerb + "\" isn't present");
        }

        Partitioning partitioning = Partitioning.HASHCODE;
        if (ctx.K_BY() != null) {
            if (ctx.S_RANDOM() != null) {
                partitioning = Partitioning.RANDOM;
            }
            if (ctx.K_SOURCE() != null) {
                partitioning = Partitioning.SOURCE;
            }
        }

        int partCount = 1;
        if (ctx.K_PARTITION() != null) {
            Object parts = Expressions.evalLoose(expression(ctx.expression(1).children, ExpressionRules.LET), variables);
            partCount = (parts instanceof Number) ? (int) parts : Utils.parseNumber(String.valueOf(parts)).intValue();
            if (partCount < 1) {
                throw new InvalidConfigurationException("CREATE DS \"" + inputName + "\" requested number of PARTITIONs below 1");
            }
        }

        String path = String.valueOf(Expressions.evalLoose(expression(ctx.expression(0).children, ExpressionRules.LET), variables));

        Map<String, Object> params = resolveParams(funcExpr.params_expr());

        if (verbose) {
            System.out.println("CREATE parameters: " + defParams(Adapters.INPUTS.get(inVerb).meta.definitions, params) + "\n");
        }

        ListOrderedMap<String, StreamInfo> si = dataContext.createDataStreams(inVerb, inputName, path, params, partCount, partitioning);

        if (verbose) {
            int ut = DataContext.usageThreshold();
            for (Map.Entry<String, StreamInfo> sii : si.entrySet()) {
                System.out.println("CREATEd DS " + sii.getKey() + ": " + sii.getValue().describe(ut));
            }
        }
    }

    private void transform(TDL4.Transform_stmtContext ctx) {
        String dsNames = resolveName(ctx.ds_name().L_IDENTIFIER());

        List<String> dataStreams;
        if (ctx.S_STAR() != null) {
            dataStreams = dataContext.getNames(dsNames + STAR);

            if (dataStreams.isEmpty()) {
                return;
            }
        } else {
            if (dataContext.has(dsNames)) {
                dataStreams = Collections.singletonList(dsNames);
            } else {
                throw new InvalidConfigurationException("TRANSFORM \"" + dsNames + "\" refers to nonexistent DataStream");
            }
        }

        TDL4.Func_exprContext funcExpr = ctx.func_expr();

        String tfVerb = resolveName(funcExpr.func().L_IDENTIFIER());
        if (!Transforms.TRANSFORMS.containsKey(tfVerb)) {
            throw new InvalidConfigurationException("TRANSFORM " + tfVerb + "() refers to unknown Transform");
        }

        TransformMeta meta = Transforms.TRANSFORMS.get(tfVerb).meta;

        for (String dsName : dataStreams) {
            StreamType from = dataContext.get(dsName).streamType;
            if ((meta.from != StreamType.Passthru) && (meta.from != from)) {
                throw new InvalidConfigurationException("TRANSFORM " + tfVerb + "() doesn't accept source DataStream type " + from);
            }
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

        Map<ObjLvl, List<String>> columns = new HashMap<>();
        for (TDL4.Columns_itemContext columnsItem : ctx.columns_item()) {
            List<String> columnList;

            if (columnsItem.var_name() != null) {
                ArrayWrap namesArr = variables.getArray(resolveName(columnsItem.var_name().L_IDENTIFIER()));
                if (namesArr == null) {
                    throw new InvalidConfigurationException("TRANSFORM attribute list references to NULL variable $" + columnsItem.var_name().L_IDENTIFIER());
                }

                columnList = Arrays.stream(namesArr.data()).map(String::valueOf).collect(Collectors.toList());
            } else {
                columnList = columnsItem.L_IDENTIFIER().stream().map(this::resolveName).collect(Collectors.toList());
            }

            TDL4.Type_columnsContext columnsType = columnsItem.type_columns();

            switch (requested) {
                case PlainText:
                case Columnar:
                case Structured: {
                    if ((columnsType == null) || (columnsType.T_VALUE() != null)) {
                        columns.put(ObjLvl.VALUE, columnList);
                    }
                    break;
                }
                case Point: {
                    if ((columnsType.T_POINT() != null)) {
                        columns.put(ObjLvl.POINT, columnList);
                    }
                    break;
                }
                case Track: {
                    if (columnsType.T_TRACK() != null) {
                        columns.put(ObjLvl.TRACK, columnList);
                    } else if (columnsType.T_POINT() != null) {
                        columns.put(ObjLvl.POINT, columnList);
                    } else if (columnsType.T_SEGMENT() != null) {
                        columns.put(ObjLvl.SEGMENT, columnList);
                    }
                    break;
                }
                case Polygon: {
                    if (columnsType.T_POLYGON() != null) {
                        columns.put(ObjLvl.POLYGON, columnList);
                    }
                    break;
                }
                case Passthru: {
                    if ((columnsType == null) || (columnsType.T_VALUE() != null)) {
                        columns.put(ObjLvl.VALUE, columnList);
                    } else if (columnsType.T_TRACK() != null) {
                        columns.put(ObjLvl.TRACK, columnList);
                    } else if (columnsType.T_POINT() != null) {
                        columns.put(ObjLvl.POINT, columnList);
                    } else if (columnsType.T_SEGMENT() != null) {
                        columns.put(ObjLvl.SEGMENT, columnList);
                    } else if (columnsType.T_POLYGON() != null) {
                        columns.put(ObjLvl.POLYGON, columnList);
                    }
                    break;
                }
            }
        }

        TDL4.Key_itemContext keyExpr = ctx.key_item();
        List<Expressions.ExprItem<?>> keyExpression;
        String ke;
        if (keyExpr != null) {
            keyExpression = expression(keyExpr.attr_expr().children, ExpressionRules.QUERY);
            ke = keyExpr.attr_expr().getText();
        } else {
            keyExpression = Collections.emptyList();
            ke = null;
        }

        StreamConverter converter;
        try {
            converter = Transforms.TRANSFORMS.get(tfVerb).configurable.getDeclaredConstructor().newInstance().converter();
        } catch (Exception e) {
            throw new InvalidConfigurationException("Unable to initialize TRANSFORM " + tfVerb + "()");
        }

        boolean repartition = false;
        int partCount = 0;
        if (ctx.K_PARTITION() != null) {
            repartition = true;

            if (ctx.expression() != null) {
                Object parts = Expressions.evalLoose(expression(ctx.expression().children, ExpressionRules.LET), variables);
                partCount = (parts instanceof Number) ? ((Number) parts).intValue() : Utils.parseNumber(String.valueOf(parts)).intValue();
                if (partCount < 1) {
                    throw new InvalidConfigurationException("TRANSFORM \"" + dsNames + "\" requested number of PARTITIONs below 1");
                }
            }
        }

        int ut = DataContext.usageThreshold();
        for (String dsName : dataStreams) {
            if (verbose) {
                System.out.println("TRANSFORMing DS " + dsName + ": " + dataContext.streamInfo(dsName).describe(ut));
                try {
                    System.out.println("TRANSFORM parameters: " + defParams(meta.definitions, params) + "\n");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            StreamInfo si = dataContext.alterDataStream(dsName, converter, columns, keyExpression, ke, meta.keyAfter(),
                    repartition, partCount,
                    new Configuration(meta.definitions, "Transform '" + tfVerb + "'", params), variables);
            if (verbose) {
                System.out.println("TRANSFORMed DS " + dsName + ": " + si.describe(ut));
            }
        }
    }

    private void copy(TDL4.Copy_stmtContext ctx) {
        String outputName = resolveName(ctx.ds_parts().L_IDENTIFIER());

        List<String> dataStreams;
        if (ctx.S_STAR() != null) {
            dataStreams = dataContext.getNames(outputName + STAR);

            if (dataStreams.isEmpty()) {
                return;
            }
        } else {
            if (dataContext.has(outputName)) {
                dataStreams = Collections.singletonList(outputName);
            } else {
                throw new InvalidConfigurationException("COPY DS \"" + outputName + "\" refers to nonexistent DataStream");
            }
        }

        TDL4.Func_exprContext funcExpr = ctx.func_expr();
        String outVerb = resolveName(funcExpr.func().L_IDENTIFIER());

        if (!Adapters.OUTPUTS.containsKey(outVerb)) {
            throw new InvalidConfigurationException("Storage output adapter \"" + outVerb + "\" isn't present");
        }

        OutputAdapterMeta meta = Adapters.OUTPUTS.get(outVerb).meta;
        List<StreamType> types = Arrays.asList(meta.type);
        for (String dsName : dataStreams) {
            StreamType streamType = dataContext.get(dsName).streamType;
            if (!types.contains(streamType)) {
                throw new InvalidConfigurationException("Storage output adapter \"" + outVerb + "\" doesn't support DataStream \"" + dsName + "\" of type " + streamType);
            }
        }

        String path = String.valueOf(Expressions.evalLoose(expression(ctx.expression().children, ExpressionRules.LET), variables));

        Map<String, Object> params = resolveParams(funcExpr.params_expr());
        int ut = DataContext.usageThreshold();
        for (String dataStream : dataStreams) {
            if (verbose) {
                System.out.println("COPYing DS " + dataStream + ": " + dataContext.streamInfo(dataStream).describe(ut));
                try {
                    System.out.println("COPY parameters: " + defParams(meta.definitions, params) + "\n");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            int[] partitions = null;
            if (ctx.ds_parts().K_PARTITION() != null) {
                partitions = getParts(ctx.ds_parts().expression().children, variables);
            }

            dataContext.copyDataStream(outVerb, dataStream, path, params, partitions);

            if (verbose) {
                System.out.println("Lineage:");
                for (StreamLineage sl : dataContext.get(dataStream).lineage) {
                    System.out.println("\t" + sl.toString());
                }
            }
        }
    }

    private int[] getParts(List<ParseTree> children, VariablesContext variables) {
        int[] partitions;

        Object parts = Expressions.evalLoose(expression(children, ExpressionRules.LET), variables);
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
            value = Expressions.evalLoose(expression(ctx.expression().children, ExpressionRules.LET), variables);
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

        Object expr = Expressions.evalLoose(expression(ctx.expression().children, ExpressionRules.LET), variables);
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

        boolean then = Expressions.boolLoose(expression(expr.children, ExpressionRules.LET), variables);
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

            if (child instanceof TDL4.Func_attrContext funcAttr) {
                if (funcAttr.attr_expr() != null) {
                    for (TDL4.Attr_exprContext e : funcAttr.attr_expr()) {
                        predExpStack.addAll(doShuntingYard(e.children));
                    }
                }

                if (funcAttr.func() != null) {
                    predExpStack.add(funcAttr);
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
                    case QUERY: {
                        String propName = resolveName(((TDL4.AttrContext) exprItem).L_IDENTIFIER());

                        items.add(Expressions.attrItem(propName));
                        continue;
                    }
                    case AT: {
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
                Operator<?> eo = Operators.get(exprItem.getText());
                if (eo == null) {
                    throw new RuntimeException("Unknown operator token " + exprItem.getText());
                } else {
                    int arity = eo.arity();
                    items.add(Expressions.stackGetter(arity));
                    items.add(Expressions.opItem(eo));
                }

                continue;
            }

            if (exprItem instanceof TDL4.ArrayContext array) {
                Object[] values = null;
                if (array.S_RANGE() != null) {
                    long a = resolveNumericLiteral(array.L_NUMERIC(0)).longValue();
                    long b = resolveNumericLiteral(array.L_NUMERIC(1)).longValue();

                    if (a > b) {
                        values = LongStream.rangeClosed(b, a).boxed().toArray();
                        ArrayUtils.reverse(values);
                    }
                    values = LongStream.rangeClosed(a, b).boxed().toArray();
                } else {
                    if ((rules == ExpressionRules.AT) || (rules == ExpressionRules.LET)) {
                        if (!array.L_IDENTIFIER().isEmpty()) {
                            values = array.L_IDENTIFIER().stream()
                                    .map(this::resolveName)
                                    .toArray(String[]::new);
                        }
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
                Function<?> ef = Functions.get(resolveName(funcCtx.L_IDENTIFIER()));
                if (ef == null) {
                    throw new RuntimeException("Unknown function token " + exprItem.getText());
                } else {
                    int arity = ef.arity();

                    if ((arity < Function.ARBITR_ARY) && (rules != ExpressionRules.QUERY)) {
                        throw new RuntimeException("Record-related function " + ef.name() + " can't be called outside of query context");
                    }

                    if ((arity == Function.ARBITR_ARY) || (arity == Function.RECORD_LEVEL)) {
                        items.add(Expressions.stackGetter(funcCall.expression().size()));
                    } else if (arity > 0) {
                        items.add(Expressions.stackGetter(arity));
                    }
                    items.add(Expressions.funcItem(ef));
                }

                continue;
            }

            if (exprItem instanceof TDL4.Func_attrContext funcAttr) {
                TDL4.FuncContext funcCtx = funcAttr.func();
                Function<?> ef = Functions.get(resolveName(funcCtx.L_IDENTIFIER()));
                if (ef == null) {
                    throw new RuntimeException("Unknown function token " + exprItem.getText());
                } else {
                    int arity = ef.arity();

                    if ((arity < Function.ARBITR_ARY) && (rules != ExpressionRules.QUERY)) {
                        throw new RuntimeException("Record-related function " + ef.name() + " can't be called outside of query context");
                    }

                    switch (arity) {
                        case Function.RECORD_KEY: {
                            items.add(Expressions.keyItem(funcAttr.attr_expr().size()));
                            break;
                        }
                        case Function.RECORD_OBJECT: {
                            items.add(Expressions.objItem(funcAttr.attr_expr().size()));
                            break;
                        }
                        case Function.WHOLE_RECORD: {
                            items.add(Expressions.recItem(funcAttr.attr_expr().size()));
                            break;
                        }
                        case Function.RECORD_LEVEL:
                        case Function.ARBITR_ARY: {
                            items.add(Expressions.stackGetter(funcAttr.attr_expr().size()));
                            break;
                        }
                        case Function.NO_ARGS: {
                            break;
                        }
                        default: {
                            items.add(Expressions.stackGetter(arity));
                        }
                    }
                    items.add(Expressions.funcItem(ef));
                }

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
        String intoName = resolveName(ctx.ds_name().L_IDENTIFIER());
        if (dataContext.has(intoName)) {
            throw new InvalidConfigurationException("SELECT INTO \"" + intoName + "\" tries to create DataStream \"" + intoName + "\" which already exists");
        }

        TDL4.From_scopeContext from = ctx.from_scope();

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

            if (from.union_op().S_XOR() != null) {
                union = UnionSpec.XOR;
            } else if (from.union_op().S_AND() != null) {
                union = UnionSpec.AND;
            } else {
                union = UnionSpec.CONCAT;
            }
        }

        ListOrderedMap<String, int[]> fromList = new ListOrderedMap<>();
        for (TDL4.Ds_partsContext e : from.ds_parts()) {
            String s = resolveName(e.L_IDENTIFIER());
            fromList.put(s, (e.K_PARTITION() != null) ? getParts(e.expression().children, variables) : null);
        }
        if (starFrom) {
            dataContext.getNames(fromList.get(0) + STAR);
        }

        List<SelectItem> items = new ArrayList<>();

        DataStream firstStream = dataContext.get(fromList.get(0));

        boolean star = (ctx.S_STAR() != null);
        if (star) {
            if (join != null) {
                for (String fromName : fromList.keyList()) {
                    List<String> attributes = dataContext.get(fromName).attributes(ObjLvl.VALUE);
                    for (String attr : attributes) {
                        items.add(new SelectItem(null, fromName + "." + attr, ObjLvl.VALUE));
                    }
                }
            } else {
                for (Map.Entry<ObjLvl, List<String>> attr : firstStream.attributes().entrySet()) {
                    attr.getValue().forEach(a -> items.add(new SelectItem(null, a, attr.getKey())));
                }
            }
        } else {
            List<TDL4.What_exprContext> what = ctx.what_expr();

            for (TDL4.What_exprContext expr : what) {
                List<ParseTree> exprTree = expr.attr_expr().children;
                List<Expressions.ExprItem<?>> item = expression(exprTree, ExpressionRules.QUERY);

                String alias;
                TDL4.AliasContext aliasCtx = expr.alias();
                if (aliasCtx != null) {
                    alias = resolveName(aliasCtx.L_IDENTIFIER());
                } else {
                    if ((exprTree.size() == 1) && (exprTree.get(0) instanceof TDL4.AttrContext)) {
                        alias = resolveName(((TDL4.AttrContext) exprTree.get(0)).L_IDENTIFIER());
                    } else {
                        alias = expr.attr_expr().getText();
                    }
                }

                ObjLvl typeAlias = resolveType(expr.type_alias());
                items.add(new SelectItem(item, alias, typeAlias));
            }
        }

        Long limitRecords = null;
        Double limitPercent = null;

        if (ctx.limit_expr() != null) {
            String limit = String.valueOf(Expressions.evalLoose(expression(ctx.limit_expr().expression().children, ExpressionRules.LET), variables));

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
            List<Expressions.ExprItem<?>> expr = expression(whereCtx.attr_expr().children, ExpressionRules.QUERY);
            ObjLvl category = resolveType(whereCtx.type_alias());
            whereItem = new WhereItem(expr, category);
        }

        int ut = DataContext.usageThreshold();

        JavaPairRDD<Object, DataRecord<?>> result;
        Map<ObjLvl, List<String>> resultColumns;
        if (star && (union == null) && (join == null) && (whereItem.expression == null)) {
            if (verbose) {
                System.out.println("Duplicated DS " + fromList.get(0) + ": " + dataContext.streamInfo(fromList.get(0)).describe(ut));
            }

            result = dataContext.rdd(firstStream);
            resultColumns = firstStream.attributes();
        } else {
            if (verbose) {
                for (String fromName : fromList.keyList()) {
                    System.out.println("SELECTed FROM DS " + fromName + ": " + dataContext.streamInfo(fromName).describe(ut));
                }
            }

            result = dataContext.select(fromList, union, join, star, items, whereItem, variables);
            resultColumns = new HashMap<>();
            for (SelectItem item : items) {
                resultColumns.compute(item.category, (k, v) -> {
                    if (v == null) {
                        v = new ArrayList<>();
                    }
                    v.add(item.alias);
                    return v;
                });
            }
        }

        if (ctx.K_DISTINCT() != null) {
            result = result.distinct();
        }

        if (limitRecords != null) {
            result = result.sample(false, limitRecords.doubleValue() / result.count());
        }
        if (limitPercent != null) {
            result = result.sample(false, limitPercent);
        }

        DataStream resultDs = new DataStreamBuilder(intoName, resultColumns)
                .generated("SELECT", firstStream.streamType, dataContext.getAll(fromList.keyList().toArray(new String[0])).valueList().toArray(new DataStream[0]))
                .build(result);

        dataContext.put(intoName, resultDs);

        if (verbose) {
            JavaPairRDD<Object, DataRecord<?>> rdd = dataContext.rdd(resultDs);
            System.out.println("SELECTing INTO DS " + intoName + ": " + new StreamInfo(resultDs.attributes(), resultDs.keyExpr, rdd.getStorageLevel().description(),
                    resultDs.streamType.name(), rdd.getNumPartitions(), resultDs.getUsages()).describe(ut));
        }
    }

    private Collection<?> subQuery(TDL4.Sub_queryContext subQuery) {
        boolean distinct = subQuery.K_DISTINCT() != null;

        String input = resolveName(subQuery.ds_parts().L_IDENTIFIER());
        if (!dataContext.has(input)) {
            throw new InvalidConfigurationException("LET with SELECT refers to nonexistent DataStream \"" + input + "\"");
        }

        TDL4.What_exprContext what = subQuery.what_expr();
        List<Expressions.ExprItem<?>> item = expression(what.attr_expr().children, ExpressionRules.QUERY);

        List<Expressions.ExprItem<?>> query = new ArrayList<>();
        if (subQuery.where_expr() != null) {
            query = expression(subQuery.where_expr().attr_expr().children, ExpressionRules.QUERY);
        }

        Long limitRecords = null;
        Double limitPercent = null;

        if (subQuery.limit_expr() != null) {
            String limit = String.valueOf(Expressions.evalLoose(expression(subQuery.limit_expr().expression().children, ExpressionRules.LET), variables));

            if (subQuery.limit_expr().S_PERCENT() != null) {
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

        int[] partitions = null;
        if (subQuery.ds_parts().K_PARTITION() != null) {
            Object parts = Expressions.evalLoose(expression(subQuery.ds_parts().expression().children, ExpressionRules.LET), variables);
            if (parts instanceof ArrayWrap) {
                partitions = Arrays.stream(((ArrayWrap) parts).data()).mapToInt(p -> Integer.parseInt(String.valueOf(p))).toArray();
            } else {
                partitions = new int[]{Integer.parseInt(String.valueOf(parts))};
            }
        }

        return dataContext.subQuery(distinct, dataContext.get(input), partitions, item, query, limitPercent, limitRecords, variables);
    }

    private void call(TDL4.Call_stmtContext ctx) {
        TDL4.Func_exprContext funcExpr = ctx.func_expr();

        String verb = resolveName(funcExpr.func().L_IDENTIFIER());
        Map<String, Object> params = resolveParams(funcExpr.params_expr());
        if (ctx.operation_io() != null) {
            if (!Operations.OPERATIONS.containsKey(verb)) {
                throw new InvalidConfigurationException("CALL \"" + verb + "\"() refers to unknown Operation");
            }

            callOperation(verb, params, ctx.operation_io());
        } else {
            if (!library.procedures.containsKey(verb)) {
                throw new InvalidConfigurationException("CALL \"" + verb + "\"() refers to undefined PROCEDURE");
            }

            callProcedure(verb, params);
        }
    }

    private void callOperation(String opVerb, Map<String, Object> params, TDL4.Operation_ioContext ctx) {
        OperationMeta meta = Operations.OPERATIONS.get(opVerb).meta;
        if (meta.definitions != null) {
            for (Map.Entry<String, DefinitionMeta> defEntry : meta.definitions.entrySet()) {
                String name = defEntry.getKey();
                DefinitionMeta def = defEntry.getValue();

                if (!def.optional && !params.containsKey(name)) {
                    throw new InvalidConfigurationException("CALL \"" + opVerb + "\"() must have mandatory parameter @" + name + " set");
                }
            }
        }

        int prefixLen = 0;
        ListOrderedMap<String, DataStream> inputMap;
        List<String> inputList = new ArrayList<>();
        TDL4.From_namedContext fromNamed = ctx.from_named();
        if (meta.input instanceof PositionalStreamsMeta psm) {
            TDL4.From_positionalContext fromScope = ctx.from_positional();
            if (fromScope == null) {
                throw new InvalidConfigurationException("CALL " + opVerb + "() INPUT requires positional or wildcard DataStream references");
            }

            if (fromScope.S_STAR() != null) {
                String prefix = resolveName(fromScope.ds_parts(0).L_IDENTIFIER());
                prefixLen = prefix.length();
                inputMap = dataContext.getAll(prefix + STAR);

                if (inputMap.isEmpty()) {
                    throw new InvalidConfigurationException("CALL " + opVerb + "() INPUT from positional wildcard reference found zero matching DataStreams");
                }
            } else {
                inputMap = new ListOrderedMap<>();
                for (TDL4.Ds_partsContext dsCtx : fromScope.ds_parts()) {
                    String dsName = resolveName(dsCtx.L_IDENTIFIER());

                    if (dataContext.has(dsName)) {
                        inputMap.put(dsName, dataContext.get(dsName));
                    } else {
                        throw new InvalidConfigurationException("CALL " + opVerb + "() INPUT refers to unknown positional DataStream \"" + dsName + "\"");
                    }
                }
            }

            if ((psm.count > 0) && (inputMap.size() != psm.count)) {
                throw new InvalidConfigurationException("CALL " + opVerb + "() INPUT requires exactly " + psm.count + " positional DataStream reference(s)");
            }

            List<StreamType> types = Arrays.asList(psm.streams.type);
            for (Map.Entry<String, DataStream> inputDs : inputMap.entrySet()) {
                if (!types.contains(inputDs.getValue().streamType)) {
                    throw new InvalidConfigurationException("CALL " + opVerb + "() doesn't accept INPUT from positional DataStream \""
                            + inputDs.getKey() + "\" of type " + inputDs.getValue().streamType);
                }
            }

            inputList.addAll(inputMap.keyList());
        } else {
            NamedStreamsMeta nsm = (NamedStreamsMeta) meta.input;

            if (fromNamed == null) {
                throw new InvalidConfigurationException("CALL " + opVerb + "() INPUT requires aliased DataStream references");
            }

            LinkedHashMap<String, String> dsMappings = new LinkedHashMap<>();
            List<TDL4.Ds_partsContext> ds_name = fromNamed.ds_parts();
            for (int i = 0; i < ds_name.size(); i++) {
                dsMappings.put(resolveName(fromNamed.ds_alias(i).L_IDENTIFIER()),
                        resolveName(ds_name.get(i).L_IDENTIFIER()));
            }

            for (Map.Entry<String, DataStreamMeta> ns : nsm.streams.entrySet()) {
                DataStreamMeta dsm = ns.getValue();

                String alias = ns.getKey();
                if (!dsm.optional && !dsMappings.containsKey(alias)) {
                    throw new InvalidConfigurationException("CALL " + opVerb + "() INPUT " + alias + " FROM requires a DataStream, but it wasn't supplied");
                }
            }

            inputMap = new ListOrderedMap<>();
            for (Map.Entry<String, String> dsm : dsMappings.entrySet()) {
                String alias = dsm.getKey();
                String dsName = dsm.getValue();

                if (!nsm.streams.containsKey(alias)) {
                    throw new InvalidConfigurationException("CALL " + opVerb + "() INPUT " + alias + " FROM refers to unknown DataStream \"" + dsName + "\"");
                }

                DataStream inputDs = dataContext.get(dsName);
                if (!Arrays.asList(nsm.streams.get(alias).type).contains(inputDs.streamType)) {
                    throw new InvalidConfigurationException("CALL " + opVerb + "() doesn't accept INPUT " + alias + " FROM  DataStream \""
                            + dsName + "\" of type " + inputDs.streamType);
                }

                inputMap.put(alias, inputDs);
                inputList.add(dsName);
            }
        }

        ListOrderedMap<String, String> outputMap = new ListOrderedMap<>();
        if (meta.output instanceof PositionalStreamsMeta psm) {
            TDL4.Into_positionalContext intoScope = ctx.into_positional();
            if (intoScope == null) {
                throw new InvalidConfigurationException("CALL " + opVerb + "() OUTPUT requires positional DataStream reference");
            }

            if (intoScope.S_STAR() != null) {
                String prefix = resolveName(intoScope.ds_name(0).L_IDENTIFIER());

                if (prefixLen > 0) {
                    int _pl = prefixLen;
                    inputMap.keyList().stream().map(e -> prefix + e.substring(_pl)).forEach(e -> outputMap.put(e, e));
                } else {
                    inputMap.keyList().stream().map(e -> prefix + e).forEach(e -> outputMap.put(e, e));
                }
            } else {
                intoScope.ds_name().stream().map(dsn -> resolveName(dsn.L_IDENTIFIER())).forEach(e -> outputMap.put(e, e));
            }

            if ((psm.count > 0) && (outputMap.size() != psm.count)) {
                throw new InvalidConfigurationException("CALL " + opVerb + "() OUTPUT requires exactly " + psm.count + " positional DataStream reference(s)");
            }

            for (String outputName : outputMap.values()) {
                if (dataContext.has(outputName)) {
                    throw new InvalidConfigurationException("CALL " + opVerb + "() OUTPUT tries to create DataStream \"" + outputName + "\" which already exists");
                }
            }
        } else {
            NamedStreamsMeta nsm = (NamedStreamsMeta) meta.output;

            TDL4.Into_namedContext intoScope = ctx.into_named();
            if (intoScope == null) {
                throw new InvalidConfigurationException("CALL " + opVerb + "() OUTPUT requires aliased DataStream references");
            }

            List<TDL4.Ds_nameContext> dsNames = intoScope.ds_name();
            List<TDL4.Ds_aliasContext> dsAliases = intoScope.ds_alias();
            for (int i = 0; i < dsNames.size(); i++) {
                outputMap.put(resolveName(dsAliases.get(i).L_IDENTIFIER()), resolveName(dsNames.get(i).L_IDENTIFIER()));
            }
            for (Map.Entry<String, String> outputName : outputMap.entrySet()) {
                if (dataContext.has(outputName.getValue())) {
                    throw new InvalidConfigurationException("CALL " + opVerb + "() OUTPUT " + outputName.getKey() + " INTO tries to create DataStream \"" + outputName.getValue() + "\" which already exists");
                }
            }

            for (Map.Entry<String, DataStreamMeta> ns : nsm.streams.entrySet()) {
                DataStreamMeta dsm = ns.getValue();

                String dsAlias = ns.getKey();
                if (!dsm.optional && !outputMap.containsKey(dsAlias)) {
                    throw new InvalidConfigurationException("CALL " + opVerb + "() OUTPUT " + dsAlias + " INTO requires DataStream reference, but it wasn't supplied");
                }
            }
        }

        int ut = DataContext.usageThreshold();

        if (verbose) {
            for (String inpName : inputList) {
                System.out.println("CALLing with INPUT DS " + inpName + ": " + dataContext.streamInfo(inpName).describe(ut));
            }

            try {
                System.out.println("CALL parameters: " + defParams(meta.definitions, params) + "\n");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        ListOrderedMap<String, DataStream> result;
        try {
            Operation op = Operations.OPERATIONS.get(opVerb).configurable.getDeclaredConstructor().newInstance();
            op.initialize(inputMap, new Configuration(Operations.OPERATIONS.get(opVerb).meta.definitions, "Operation '" + opVerb + "'", params), outputMap);
            result = op.execute();
        } catch (Exception e) {
            throw new InvalidConfigurationException("CALL " + opVerb + "() failed with an exception", e);
        }

        for (DataStream output : result.valueList()) {
            if (dataContext.has(output.name)) {
                throw new InvalidConfigurationException("CALL " + opVerb + "() OUTPUT tries to create DataStream \"" + output.name + "\" which already exists");
            } else {
                dataContext.put(output.name, output);

                if (verbose) {
                    System.out.println("CALLed with OUTPUT DS " + output.name + ": " + dataContext.streamInfo(output.name).describe(ut));
                }
            }
        }
    }

    private void callProcedure(String procName, Map<String, Object> params) {
        if (verbose) {
            System.out.println("CALLing PROCEDURE " + procName + " with params " + params + "\n");
        }

        Procedure proc = library.procedures.get(procName);

        for (Map.Entry<String, Procedure.Param> defEntry : proc.params.entrySet()) {
            String name = defEntry.getKey();
            Procedure.Param paramDef = defEntry.getValue();

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
        String dsName = resolveName(ctx.ds_name().L_IDENTIFIER());
        if (ctx.S_STAR() != null) {
            dsName += STAR;
        }

        TDL4.Key_itemContext keyExpr = ctx.key_item();
        List<Expressions.ExprItem<?>> keyExpression;
        String ke;
        if (keyExpr != null) {
            keyExpression = expression(keyExpr.attr_expr().children, ExpressionRules.QUERY);
            ke = keyExpr.attr_expr().getText();
        } else {
            keyExpression = Collections.emptyList();
            ke = null;
        }

        ListOrderedMap<String, DataStream> dataStreams = dataContext.getAll(dsName);

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

        if (Operations.OPERATIONS.containsKey(procName)) {
            throw new InvalidConfigurationException("Attempt to CREATE PROCEDURE which overrides OPERATION \"" + procName + "\"");
        }

        if ((ctx.K_REPLACE() == null) && library.procedures.containsKey(procName)) {
            throw new InvalidConfigurationException("PROCEDURE " + procName + " has already been defined. Offending definition at line " + ctx.K_CREATE().getSymbol().getLine());
        }

        Procedure.Builder proc = Procedure.builder(ctx.statements());
        for (TDL4.Proc_paramContext procParam : ctx.proc_param()) {
            if (procParam.param() == null) {
                proc.mandatory(resolveName(procParam.L_IDENTIFIER()));
            } else {
                proc.optional(resolveName(procParam.param().L_IDENTIFIER()), Expressions.evalLoose(expression(procParam.param().attr_expr().children, ExpressionRules.LET), variables));
            }
        }
        library.procedures.put(procName, proc.build());
    }

    private void dropProcedure(TDL4.Drop_procContext ctx) {
        for (TDL4.FuncContext func : ctx.func()) {
            String procName = resolveName(func.L_IDENTIFIER());

            library.procedures.remove(procName);
        }
    }

    private Map<String, Object> resolveParams(TDL4.Params_exprContext params) {
        Map<String, Object> ret = new HashMap<>();

        if (params != null) {
            for (TDL4.ParamContext atRule : params.param()) {
                Object obj = Expressions.evalLoose(expression(atRule.attr_expr().children, ExpressionRules.AT), variables);

                ret.put(resolveName(atRule.L_IDENTIFIER()), obj);
            }
        }

        return ret;
    }

    private boolean isHigher(ParseTree o1, ParseTree o2) {
        Operator<?> first = Operators.get(o1.getText());
        if (o1 instanceof TDL4.In_opContext) {
            first = Operators.IN;
        }
        if (o1 instanceof TDL4.Is_opContext) {
            first = Operators.IS;
        }
        if (o1 instanceof TDL4.Between_opContext) {
            first = Operators.BETWEEN;
        }

        Operator<?> second = Operators.get(o2.getText());
        if (o2 instanceof TDL4.In_opContext) {
            second = Operators.IN;
        }
        if (o2 instanceof TDL4.Is_opContext) {
            second = Operators.IS;
        }
        if (o2 instanceof TDL4.Between_opContext) {
            second = Operators.BETWEEN;
        }

        return ((second.prio() - first.prio()) > 0) || ((first.prio() == second.prio()) && !first.rightAssoc());
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

    private ObjLvl resolveType(TDL4.Type_aliasContext type_aliasContext) {
        if (type_aliasContext != null) {
            if (type_aliasContext.T_TRACK() != null) {
                return ObjLvl.TRACK;
            }
            if (type_aliasContext.T_SEGMENT() != null) {
                return ObjLvl.SEGMENT;
            }
            if (type_aliasContext.T_POINT() != null) {
                return ObjLvl.POINT;
            }
            if (type_aliasContext.T_POLYGON() != null) {
                return ObjLvl.POLYGON;
            }
        }
        return ObjLvl.VALUE;
    }

    private enum ExpressionRules {
        LET,
        AT,
        QUERY
    }
}
