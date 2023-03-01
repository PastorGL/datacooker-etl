/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.scripting;

import java.io.Serializable;
import java.util.List;

import static io.github.pastorgl.datacooker.Constants.OBJLVL_VALUE;

public class QueryItem implements Serializable {
    public final List<Expression<?>> expression;
    public final String category;

    public QueryItem(List<Expression<?>> expression, String category) {
        this.expression = expression;
        this.category = (category == null) ? OBJLVL_VALUE : category;
    }

    public QueryItem() {
        expression = null;
        category = null;
    }
}
