/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.geohashing.functions;

import io.github.pastorgl.datacooker.data.Record;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;
import scala.Tuple3;

import java.util.Iterator;

public abstract class HasherFunction implements PairFlatMapFunction<Iterator<Tuple2<Object, Tuple3<Double, Double, Record<?>>>>, Object, Tuple2<Record<?>, String>> {
    protected int level;

    protected HasherFunction(int level) {
        this.level = level;
    }
}
