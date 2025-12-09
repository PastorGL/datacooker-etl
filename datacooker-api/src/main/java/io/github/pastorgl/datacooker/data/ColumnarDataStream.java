/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.data;

import org.apache.commons.collections4.map.SingletonMap;
import org.apache.spark.api.java.JavaPairRDD;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.github.pastorgl.datacooker.data.ObjLvl.VALUE;

public class ColumnarDataStream extends DataStream {
    private final List<String> columns = new ArrayList<>();

    public ColumnarDataStream(String name, JavaPairRDD<Object, DataRecord<?>> rdd, List<StreamLineage> lineage, Map<ObjLvl, List<String>> columns, String keyExpr) {
        super(name, StreamType.Columnar, rdd, lineage, keyExpr);

        if (columns.containsKey(VALUE)) {
            this.columns.addAll(columns.get(VALUE));
        }
    }

    public List<String> attributes(ObjLvl objLvl) {
        return columns;
    }

    @Override
    public Map<ObjLvl, List<String>> attributes() {
        return new SingletonMap<>(VALUE, columns);
    }

    @Override
    public DataRecord<?> itemTemplate() {
        return new Columnar();
    }
}
