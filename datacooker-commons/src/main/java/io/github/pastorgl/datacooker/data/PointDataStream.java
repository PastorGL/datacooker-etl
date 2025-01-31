/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.data;

import io.github.pastorgl.datacooker.data.spatial.PointEx;
import org.apache.spark.api.java.JavaPairRDD;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.github.pastorgl.datacooker.data.ObjLvl.POINT;
import static io.github.pastorgl.datacooker.data.ObjLvl.VALUE;

public class PointDataStream extends DataStream {
    protected final Map<ObjLvl, List<String>> properties = new HashMap<>();

    public PointDataStream(String name, JavaPairRDD<Object, DataRecord<?>> rdd, List<StreamLineage> lineage, Map<ObjLvl, List<String>> properties, String keyExpr) {
        super(name, StreamType.Point, rdd, lineage, keyExpr);

        if (properties.containsKey(VALUE)) {
            this.properties.put(POINT, properties.get(VALUE));
        }
        if (properties.containsKey(POINT)) {
            this.properties.put(POINT, properties.get(POINT));
        }
    }

    public List<String> attributes(ObjLvl objLvl) {
        if (VALUE.equals(objLvl)) {
            if (properties.containsKey(POINT)) {
                return properties.get(POINT);
            }
        }
        return properties.getOrDefault(objLvl, Collections.emptyList());
    }

    @Override
    public Map<ObjLvl, List<String>> attributes() {
        return properties;
    }

    @Override
    public DataRecord<?> itemTemplate() {
        return new PointEx();
    }
}
