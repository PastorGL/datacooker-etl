/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.math.functions.attrs;

import io.github.pastorgl.datacooker.data.Record;

public class MinFunction extends AttrsFunction {
    public MinFunction(String[] columnsForCalculation) {
        super(columnsForCalculation);
    }

    @Override
    public double calcValue(Record row) {
        double result = Double.POSITIVE_INFINITY;
        for (String column : columnsForCalculation) {
            result = Math.min(result, row.asDouble(column));
        }

        return result;
    }
}
