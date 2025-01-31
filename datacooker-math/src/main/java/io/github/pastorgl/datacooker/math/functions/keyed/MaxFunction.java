/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.math.functions.keyed;

import java.util.List;

public class MaxFunction extends KeyedFunction {
    public MaxFunction(Double ceil) {
        super(ceil);
    }

    @Override
    public Double calcSeries(List<Double[]> series, int idx) {
        double result = Double.NEGATIVE_INFINITY;

        for (Double[] value : series) {
            result = Math.max(result, value[idx]);
        }
        if ((_const != null) && (_const < result)) {
            result = _const;
        }

        return result;
    }
}
