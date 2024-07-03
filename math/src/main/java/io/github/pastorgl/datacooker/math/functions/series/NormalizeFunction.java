/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.math.functions.series;

import io.github.pastorgl.datacooker.data.Record;
import org.apache.spark.api.java.JavaDoubleRDD;

import java.util.List;

public class NormalizeFunction extends SeriesFunction {
    private double max;
    private double range;

    public NormalizeFunction(String calcProp, Double upper) {
        super(calcProp, upper);
    }

    @Override
    public void calcSeries(JavaDoubleRDD series) {
        max = series.max();
        range = max - series.min();
    }

    @Override
    public Double calcValue(Record<?> row) {
        return _const - (max - row.asDouble(calcProp)) / range * _const;
    }
}
