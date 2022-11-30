/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.math.functions.keyed;

import java.util.List;
import java.util.stream.Collectors;

public class QuantileFunction extends KeyedFunction {
    public QuantileFunction(Double quantile) {
        super(quantile);
    }

    @Override
    public Double calcSeries(List<Double[]> series, int idx) {
        List<Double> serie = series.stream().map(e -> e[idx]).sorted().collect(Collectors.toList());

        int size = serie.size();
        int m = (int) (size * _const);
        return (size % 2 == 0)
                ? (serie.get(m) + serie.get(m - 1)) / 2.D
                : serie.get(m);
    }
}
