/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.math.functions.keyed;

import java.util.List;

public class AverageFunction extends KeyedFunction {
    public AverageFunction(Double shift) {
        super(shift);
    }

    @Override
    public Double calcSeries(List<Double[]> series, int idx) {
        double result = (_const == null) ? 0.D : _const;

        for (Double[] value : series) {
            result += value[idx];
        }

        return result / series.size();
    }
}
