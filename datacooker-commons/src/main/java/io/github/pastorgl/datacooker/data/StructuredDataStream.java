/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.data;

import org.apache.commons.collections4.map.SingletonMap;
import org.apache.spark.api.java.JavaPairRDD;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.github.pastorgl.datacooker.data.ObjLvl.VALUE;

public class StructuredDataStream extends DataStream {
    private final HashMap<String, Integer> columns = new HashMap<>();

    public StructuredDataStream(String name, JavaPairRDD<Object, DataRecord<?>> rdd, List<StreamLineage> lineage, Map<ObjLvl, List<String>> propNames, String keyExpr) {
        super(name, StreamType.Structured, rdd, lineage, keyExpr);

        int[] n = {0};
        if (propNames.containsKey(VALUE)) {
            propNames.get(VALUE).forEach(e -> this.columns.put(e, n[0]++));
        }
    }

    public List<String> attributes(ObjLvl objLvl) {
        return new ArrayList<>(columns.keySet());
    }

    @Override
    public Map<ObjLvl, List<String>> attributes() {
        return new SingletonMap<>(VALUE, new ArrayList<>(columns.keySet()));
    }

    @Override
    public DataRecord<?> itemTemplate() {
        return new Structured();
    }
}
