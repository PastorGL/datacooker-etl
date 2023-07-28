/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.spatial.utils;

import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;

public class TrackComparator<T> implements Comparator<Tuple2<T, Double>>, Serializable {
    @Override
    public int compare(Tuple2<T, Double> o1, Tuple2<T, Double> o2) {
        return Double.compare(o1._2, o2._2);
    }
}
