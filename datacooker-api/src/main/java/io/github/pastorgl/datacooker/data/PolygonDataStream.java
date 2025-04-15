/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.data;

import io.github.pastorgl.datacooker.data.spatial.PolygonEx;
import org.apache.spark.api.java.JavaPairRDD;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.github.pastorgl.datacooker.data.ObjLvl.POLYGON;
import static io.github.pastorgl.datacooker.data.ObjLvl.VALUE;

public class PolygonDataStream extends DataStream {
    protected final Map<ObjLvl, List<String>> properties = new HashMap<>(Map.of(POLYGON, Collections.emptyList()));

    public PolygonDataStream(String name, JavaPairRDD<Object, DataRecord<?>> rdd, List<StreamLineage> lineage, Map<ObjLvl, List<String>> properties, String keyExpr) {
        super(name, StreamType.Polygon, rdd, lineage, keyExpr);

        if (properties.containsKey(VALUE)) {
            this.properties.put(POLYGON, properties.get(VALUE));
        }
        if (properties.containsKey(POLYGON)) {
            this.properties.put(POLYGON, properties.get(POLYGON));
        }
    }

    public List<String> attributes(ObjLvl objLvl) {
        if (VALUE.equals(objLvl)) {
            return properties.get(POLYGON);
        }
        return properties.getOrDefault(objLvl, Collections.emptyList());
    }

    @Override
    public Map<ObjLvl, List<String>> attributes() {
        return properties;
    }

    @Override
    public DataRecord<?> itemTemplate() {
        return new PolygonEx();
    }
}
