/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.data;

import org.apache.commons.collections4.map.ListOrderedMap;
import org.apache.commons.collections4.map.SingletonMap;
import org.apache.spark.api.java.JavaPairRDD;

import java.util.List;
import java.util.Map;

import static io.github.pastorgl.datacooker.data.ObjLvl.VALUE;

public class ColumnarDataStream extends DataStream {
    private final ListOrderedMap<String, Integer> columns = new ListOrderedMap<>();

    public ColumnarDataStream(String name, JavaPairRDD<Object, DataRecord<?>> rdd, List<StreamLineage> lineage, Map<ObjLvl, List<String>> columns, String keyExpr) {
        super(name, StreamType.Columnar, rdd, lineage, keyExpr);

        int[] n = {0};
        if (columns.containsKey(VALUE)) {
            columns.get(VALUE).forEach(e -> this.columns.put(e, n[0]++));
        }
    }

    public List<String> attributes(ObjLvl objLvl) {
        return columns.keyList();
    }

    @Override
    public Map<ObjLvl, List<String>> attributes() {
        return new SingletonMap<>(VALUE, columns.keyList());
    }

    @Override
    public DataRecord<?> itemTemplate() {
        return new Columnar();
    }
}
