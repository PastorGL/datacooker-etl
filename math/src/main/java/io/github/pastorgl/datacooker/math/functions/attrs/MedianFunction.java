/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.math.functions.attrs;

import io.github.pastorgl.datacooker.data.Record;

import java.util.Arrays;

public class MedianFunction extends AttrsFunction {
    public MedianFunction(String[] columnsForCalculation) {
        super(columnsForCalculation);
    }

    @Override
    public double calcValue(Record row) {
        double[] result = new double[columnsForCalculation.length];

        for (int j = 0; j < columnsForCalculation.length; j++) {
            result[j] = row.asDouble(columnsForCalculation[j]);
        }
        Arrays.sort(result);

        int m = columnsForCalculation.length >> 1;
        return (columnsForCalculation.length % 2 == 0)
                ? (result[m] + result[m - 1]) / 2.D
                : result[m];
    }
}
