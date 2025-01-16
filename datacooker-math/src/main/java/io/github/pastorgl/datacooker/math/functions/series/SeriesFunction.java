/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.math.functions.series;

import io.github.pastorgl.datacooker.data.DataRecord;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static io.github.pastorgl.datacooker.math.operations.SeriesMathOperation.GEN_RESULT;

public abstract class SeriesFunction implements PairFlatMapFunction<Iterator<Tuple2<Object, DataRecord<?>>>, Object, DataRecord<?>> {
    protected final String calcProp;
    protected final Double _const;

    public SeriesFunction(String calcProp, Double _const) {
        this.calcProp = calcProp;
        this._const = _const;
    }

    public abstract void calcSeries(JavaDoubleRDD series);

    public abstract Double calcValue(DataRecord<?> row);

    @Override
    final public Iterator<Tuple2<Object, DataRecord<?>>> call(Iterator<Tuple2<Object, DataRecord<?>>> it) {
        List<Tuple2<Object, DataRecord<?>>> ret = new ArrayList<>();

        while (it.hasNext()) {
            Tuple2<Object, DataRecord<?>> row = it.next();

            DataRecord<?> rec = (DataRecord<?>) row._2.clone();
            rec.put(GEN_RESULT, calcValue(rec));

            ret.add(new Tuple2<>(row._1, rec));
        }

        return ret.iterator();
    }
}
