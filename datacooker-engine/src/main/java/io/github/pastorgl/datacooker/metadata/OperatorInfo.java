/**
 * Copyright (C) 2025 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import io.github.pastorgl.datacooker.scripting.Operator;

public class OperatorInfo extends EvaluatorInfo {
    public final int priority;
    public final Boolean rightAssoc;
    public final Boolean handleNull;

    public final Operator<?> instance;

    public OperatorInfo(Operator<?> operator) {
        super(operator);

        this.priority = operator.prio();
        this.rightAssoc = operator.rightAssoc();
        this.handleNull = operator.handleNull();

        this.instance = operator;
    }

    @JsonCreator
    public OperatorInfo(String symbol, String descr, int arity, String[] argTypes, String resultType, int priority, Boolean rightAssoc, Boolean handleNull) {
        super(symbol, descr, arity, argTypes, resultType);

        this.priority = priority;
        this.rightAssoc = rightAssoc;
        this.handleNull = handleNull;

        this.instance = null;
    }
}
