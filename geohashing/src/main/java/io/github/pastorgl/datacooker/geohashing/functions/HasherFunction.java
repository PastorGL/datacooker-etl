/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.geohashing.functions;

import io.github.pastorgl.datacooker.data.Record;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;
import scala.Tuple3;

import java.util.Iterator;

public abstract class HasherFunction implements FlatMapFunction<Iterator<Tuple3<Double, Double, Record>>, Tuple2<String, Record>> {
    protected int level;

    protected HasherFunction(int level) {
        this.level = level;
    }

    abstract public Iterator<Tuple2<String, Record>> call(Iterator<Tuple3<Double, Double, Record>> signals) throws Exception;
}
