/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.math.functions.keyed;

import java.util.List;

public class SubtractFunction extends KeyedFunction {
    public SubtractFunction(Double shift) {
        super(shift);
    }

    @Override
    public Double calcSeries(List<Double[]> series, int idx) {
        double result = series.remove(0)[idx];

        for (Double[] value : series) {
            result -= value[idx];
        }

        return (_const != null) ? (result - _const) : result;
    }
}
