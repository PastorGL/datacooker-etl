/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.math.functions.series;

import io.github.pastorgl.datacooker.data.Record;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static io.github.pastorgl.datacooker.math.SeriesMathOperation.GEN_RESULT;

public abstract class SeriesFunction implements PairFlatMapFunction<Iterator<Tuple2<Object, Record<?>>>, Object, Record<?>> {
    protected final String calcProp;
    protected final Double _const;

    public SeriesFunction(String calcProp, Double _const) {
        this.calcProp = calcProp;
        this._const = _const;
    }

    public abstract void calcSeries(JavaDoubleRDD series);

    public abstract Double calcValue(Record<?> row);

    @Override
    final public Iterator<Tuple2<Object, Record<?>>> call(Iterator<Tuple2<Object, Record<?>>> it) {
        List<Tuple2<Object, Record<?>>> ret = new ArrayList<>();

        while (it.hasNext()) {
            Tuple2<Object, Record<?>> row = it.next();

            Record<?> rec = (Record<?>) row._2.clone();
            rec.put(GEN_RESULT, calcValue(rec));

            ret.add(new Tuple2<>(row._1, rec));
        }

        return ret.iterator();
    }
}
