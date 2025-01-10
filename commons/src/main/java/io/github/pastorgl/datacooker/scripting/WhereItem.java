/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.scripting;

import io.github.pastorgl.datacooker.data.ObjLvl;

import java.io.Serializable;
import java.util.List;

public class WhereItem implements Serializable {
    public final List<Expressions.ExprItem<?>> expression;
    public final ObjLvl category;

    public WhereItem(List<Expressions.ExprItem<?>> expression, ObjLvl category) {
        this.expression = expression;
        this.category = (category == null) ? ObjLvl.VALUE : category;
    }

    public WhereItem() {
        expression = null;
        category = null;
    }
}
