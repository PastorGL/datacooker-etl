/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.math.functions.series;

import io.github.pastorgl.datacooker.data.DataRecord;
import org.apache.spark.api.java.JavaDoubleRDD;

public class StdDevFunction extends SeriesFunction {
    private double stdDev;
    private double mean;

    public StdDevFunction(String calcProp, Double ignore) {
        super(calcProp, ignore);
    }

    @Override
    public void calcSeries(JavaDoubleRDD series) {
        if (series.count() < 30) {
            stdDev = series.stdev();
        } else {
            stdDev = series.sampleStdev();
        }
        mean = series.mean();
    }

    @Override
    public Double calcValue(DataRecord<?> row) {
        return (row.asDouble(calcProp) - mean) / stdDev;
    }
}
