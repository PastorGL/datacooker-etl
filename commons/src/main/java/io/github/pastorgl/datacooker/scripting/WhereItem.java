/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.scripting;

import java.io.Serializable;
import java.util.List;

import static io.github.pastorgl.datacooker.Constants.OBJLVL_VALUE;

public class WhereItem implements Serializable {
    public final List<Expressions.ExprItem<?>> expression;
    public final String category;

    public WhereItem(List<Expressions.ExprItem<?>> expression, String category) {
        this.expression = expression;
        this.category = (category == null) ? OBJLVL_VALUE : category;
    }

    public WhereItem() {
        expression = null;
        category = null;
    }
}
