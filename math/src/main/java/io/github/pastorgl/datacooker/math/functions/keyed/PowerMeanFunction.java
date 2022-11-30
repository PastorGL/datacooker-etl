/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.math.functions.keyed;

import java.util.List;

public class PowerMeanFunction extends KeyedFunction {
    public PowerMeanFunction(Double pow) {
        super(pow);
    }

    @Override
    public Double calcSeries(List<Double[]> series, int idx) {
        double result = 0.D;

        for (Double[] value : series) {
            result += Math.pow(value[idx], _const);
        }

        return Math.pow(result / series.size(), 1.D / _const);
    }
}
