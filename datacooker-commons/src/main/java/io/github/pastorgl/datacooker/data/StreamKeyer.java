/**
 * Copyright (C) 2025 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.data;

import io.github.pastorgl.datacooker.scripting.Expressions;

import java.util.List;

@FunctionalInterface
public interface StreamKeyer {
    DataStream apply(List<Expressions.ExprItem<?>> keyExpr, DataStream ds);
}
