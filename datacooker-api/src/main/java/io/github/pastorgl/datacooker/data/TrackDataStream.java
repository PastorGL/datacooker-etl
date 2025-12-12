/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.data;

import io.github.pastorgl.datacooker.data.spatial.SegmentedTrack;
import org.apache.spark.api.java.JavaPairRDD;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.github.pastorgl.datacooker.data.ObjLvl.*;

public class TrackDataStream extends DataStream {
    protected final Map<ObjLvl, List<String>> properties = new HashMap<>(Map.of(
            TRACK, Collections.emptyList(),
            SEGMENT, Collections.emptyList(),
            POINT, Collections.emptyList()
    ));

    public TrackDataStream(String name, JavaPairRDD<Object, DataRecord<?>> rdd, List<StreamLineage> lineage, Map<ObjLvl, List<String>> properties, String keyExpr) {
        super(name, StreamType.Track, rdd, lineage, keyExpr);

        if (properties != null) {
            if (properties.containsKey(VALUE)) {
                this.properties.put(TRACK, properties.get(VALUE));
            }
            if (properties.containsKey(TRACK)) {
                this.properties.put(TRACK, properties.get(TRACK));
            }
            if (properties.containsKey(SEGMENT)) {
                this.properties.put(SEGMENT, properties.get(SEGMENT));
            }
            if (properties.containsKey(POINT)) {
                this.properties.put(POINT, properties.get(POINT));
            }
        }
    }

    public List<String> attributes(ObjLvl objLvl) {
        if (VALUE.equals(objLvl)) {
            return properties.get(TRACK);
        }
        return properties.getOrDefault(objLvl, Collections.emptyList());
    }

    @Override
    public Map<ObjLvl, List<String>> attributes() {
        return properties;
    }

    @Override
    public DataRecord<?> itemTemplate() {
        return new SegmentedTrack();
    }
}
