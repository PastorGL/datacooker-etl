/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.geohashing.functions;

import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;
import scala.Tuple3;

import java.util.Iterator;

public abstract class HasherFunction<T> implements FlatMapFunction<Iterator<Tuple3<Double, Double, T>>, Tuple2<String, T>> {
    protected int level;

    protected HasherFunction(int level) {
        this.level = level;
    }

    abstract public Iterator<Tuple2<String, T>> call(Iterator<Tuple3<Double, Double, T>> signals) throws Exception;
}
