/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.data;

import org.apache.spark.api.java.JavaPairRDD;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static io.github.pastorgl.datacooker.data.ObjLvl.VALUE;

public class PlainTextDataStream extends DataStream {
    private final Map<ObjLvl, List<String>> ATTRS = Collections.singletonMap(VALUE, Collections.singletonList("_value"));

    public PlainTextDataStream(String name, JavaPairRDD<Object, DataRecord<?>> rdd, List<StreamLineage> lineage, String keyExpr) {
        super(name, StreamType.PlainText, rdd, lineage, keyExpr);
    }

    @Override
    public Map<ObjLvl, List<String>> attributes() {
        return ATTRS;
    }

    @Override
    public List<String> attributes(ObjLvl objLvl) {
        return Collections.singletonList("_value");
    }

    @Override
    public DataRecord<?> itemTemplate() {
        return new PlainText(new byte[0]);
    }
}
