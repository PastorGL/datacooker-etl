/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.geohashing.functions;

import com.uber.h3core.H3Core;
import io.github.pastorgl.datacooker.data.Record;
import io.github.pastorgl.datacooker.spatial.utils.SpatialUtils;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

@Deprecated
public class H3Function extends HasherFunction {
    public H3Function(int level) {
        super(level);
    }

    @Override
    public Iterator<Tuple2<Object, Tuple2<Record<?>, String>>> call(Iterator<Tuple2<Object, Tuple3<Double, Double, Record<?>>>> signals) throws Exception {
        H3Core h3 = SpatialUtils.H3;

        List<Tuple2<Object, Tuple2<Record<?>, String>>> ret = new ArrayList<>();
        while (signals.hasNext()) {
            Tuple2<Object, Tuple3<Double, Double, Record<?>>> signal = signals.next();

            ret.add(new Tuple2<>(signal._1, new Tuple2<>(signal._2._3(), h3.latLngToCellAddress(signal._2._1(), signal._2._2(), level))));
        }

        return ret.iterator();
    }
}
