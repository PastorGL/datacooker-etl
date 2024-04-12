/**
 * Copyright (C) 2024 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import io.github.pastorgl.datacooker.scripting.Function;
import io.github.pastorgl.datacooker.scripting.Functions;
import io.github.pastorgl.datacooker.scripting.Operator;
import io.github.pastorgl.datacooker.scripting.Operators;

import java.io.Serializable;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class EvaluatorInfo implements Serializable {
    private static final Map<String, EvaluatorInfo> CACHE = new HashMap<>();

    public static EvaluatorInfo bySymbol(String symbol) {
        EvaluatorInfo info = null;

        if (!CACHE.containsKey(symbol)) {
            Type[] types;

            if (Operators.OPERATORS.containsKey(symbol)) {
                Operator<?> operator = Operators.OPERATORS.get(symbol);
                Class<? extends Operator> cls = operator.getClass();

                try {
                    types = ((ParameterizedType) cls.getGenericSuperclass()).getActualTypeArguments();
                } catch (Exception ignore) { // operator alias
                    types = ((ParameterizedType) cls.getSuperclass().getGenericSuperclass()).getActualTypeArguments();
                }

                info = new EvaluatorInfo(operator.name(), operator.descr(), operator.arity(), Arrays.stream(types).skip(1L).map(EvaluatorInfo::getSimpleName).toArray(String[]::new),
                        getSimpleName(types[0]), operator.prio(), operator.rightAssoc(), operator.handleNull());
            }

            if (Functions.FUNCTIONS.containsKey(symbol)) {
                Function<?> function = Functions.FUNCTIONS.get(symbol);
                Class<? extends Function> cls = function.getClass();

                try {
                    types = ((ParameterizedType) cls.getGenericSuperclass()).getActualTypeArguments();
                } catch (Exception ignore) { // function alias
                    types = ((ParameterizedType) cls.getSuperclass().getGenericSuperclass()).getActualTypeArguments();
                }

                info = new EvaluatorInfo(function.name(), function.descr(), function.arity(), Arrays.stream(types).skip(1L).map(EvaluatorInfo::getSimpleName).toArray(String[]::new),
                        getSimpleName(types[0]));
            }
        }

        if (info != null) {
            CACHE.put(symbol, info);
        }

        return CACHE.get(symbol);
    }

    private static String getSimpleName(Object a) {
        if (a instanceof Class) {
            return ((Class<?>) a).getSimpleName();
        }
        if (a instanceof ParameterizedType) {
            return ((Class<?>) ((ParameterizedType) a).getRawType()).getSimpleName();
        }

        return "Object";
    }

    public final String symbol;
    public final String descr;
    public final int arity;
    public final String[] argTypes;
    public final String resultType;
    public final int priority;
    public final Boolean rightAssoc;
    public final Boolean handleNull;

    public EvaluatorInfo(String symbol, String descr, int arity, String[] argTypes, String resultType) {
        this.symbol = symbol;
        this.descr = descr;
        this.arity = arity;
        this.argTypes = argTypes;
        this.resultType = resultType;
        this.priority = -1;
        this.rightAssoc = null;
        this.handleNull = null;
    }

    @JsonCreator
    public EvaluatorInfo(String symbol, String descr, int arity, String[] argTypes, String resultType, int priority, Boolean rightAssoc, Boolean handleNull) {
        this.symbol = symbol;
        this.descr = descr;
        this.arity = arity;
        this.argTypes = argTypes;
        this.resultType = resultType;
        this.priority = priority;
        this.rightAssoc = rightAssoc;
        this.handleNull = handleNull;
    }
}
