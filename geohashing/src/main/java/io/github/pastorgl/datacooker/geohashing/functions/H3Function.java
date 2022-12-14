/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.geohashing.functions;

import com.uber.h3core.H3Core;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class H3Function<T> extends HasherFunction<T> {
    public H3Function(int level) {
        super(level);
    }

    @Override
    public Iterator<Tuple2<String, T>> call(Iterator<Tuple3<Double, Double, T>> signals) throws Exception {
        H3Core h3 = H3Core.newInstance();

        List<Tuple2<String, T>> ret = new ArrayList<>();
        while (signals.hasNext()) {
            Tuple3<Double, Double, T> signal = signals.next();

            ret.add(new Tuple2<>(h3.geoToH3Address(signal._1(), signal._2(), level), signal._3()));
        }

        return ret.iterator();
    }
}
