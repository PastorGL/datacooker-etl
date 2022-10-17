/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.math.functions.keyed;

import java.util.List;

public class EqualityFunction extends KeyedFunction {
    public EqualityFunction(Double threshold) {
        super(threshold);
    }

    @Override
    public Double calcSeries(List<Double[]> series, int idx) {
        double value0 = series.get(0)[idx];

        for (int i = 1; i < series.size(); i++) {
            double value1 = series.get(i)[idx];

            if (_const == null) {
                if (Double.compare(value0, value1) != 0) {
                    return 0.D;
                }
            } else {
                if (Math.abs(value0 - value1) >= _const) {
                    return 0.D;
                }
            }

            value0 = value1;
        }

        return 1.D;
    }
}
