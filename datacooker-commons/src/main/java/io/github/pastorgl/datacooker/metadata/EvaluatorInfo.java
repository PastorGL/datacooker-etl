/**
 * Copyright (C) 2024 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.metadata;

import io.github.pastorgl.datacooker.scripting.Evaluator;

import java.io.Serializable;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;

public abstract class EvaluatorInfo implements Serializable {
    public final String symbol;
    public final String descr;
    public final int arity;
    public final String[] argTypes;
    public final String resultType;

    protected EvaluatorInfo(Evaluator<?> evaluator) {
        this.symbol = evaluator.name();
        this.descr = evaluator.descr();
        this.arity = evaluator.arity();

        Class<?> cls = evaluator.getClass();

        Type[] types;
        try {
            types = ((ParameterizedType) cls.getGenericSuperclass()).getActualTypeArguments();
        } catch (Exception ignore) { // function alias
            types = ((ParameterizedType) cls.getSuperclass().getGenericSuperclass()).getActualTypeArguments();
        }

        this.argTypes = Arrays.stream(types).skip(1L).map(EvaluatorInfo::getSimpleName).toArray(String[]::new);
        this.resultType = getSimpleName(types[0]);
    }

    protected EvaluatorInfo(String symbol, String descr, int arity, String[] argTypes, String resultType) {
        this.symbol = symbol;
        this.descr = descr;
        this.arity = arity;
        this.argTypes = argTypes;
        this.resultType = resultType;
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
}
