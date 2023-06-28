/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.math.functions.attrs;

import io.github.pastorgl.datacooker.data.Record;

import java.io.Serializable;

public abstract class AttrsFunction implements Serializable {
    protected final String[] columnsForCalculation;

    public AttrsFunction(String[] columnsForCalculation) {
        this.columnsForCalculation = columnsForCalculation;
    }

    public abstract double calcValue(Record row);
}
