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

public class TrackDataStream extends DataStream {
    protected final Map<ObjLvl, List<String>> properties = new HashMap<>();

    public TrackDataStream(String name, JavaPairRDD<Object, DataRecord<?>> rdd, List<StreamLineage> lineage, Map<ObjLvl, List<String>> properties, String keyExpr) {
        super(name, StreamType.Track, rdd, lineage, keyExpr);

        if (properties.containsKey(ObjLvl.VALUE)) {
            this.properties.put(ObjLvl.TRACK, properties.get(ObjLvl.VALUE));
        }
        if (properties.containsKey(ObjLvl.TRACK)) {
            this.properties.put(ObjLvl.TRACK, properties.get(ObjLvl.TRACK));
        }
        if (properties.containsKey(ObjLvl.SEGMENT)) {
            this.properties.put(ObjLvl.SEGMENT, properties.get(ObjLvl.SEGMENT));
        }
        if (properties.containsKey(ObjLvl.POINT)) {
            this.properties.put(ObjLvl.POINT, properties.get(ObjLvl.POINT));
        }
    }

    public List<String> attributes(ObjLvl objLvl) {
        if (ObjLvl.VALUE.equals(objLvl)) {
            if (properties.containsKey(ObjLvl.TRACK)) {
                return properties.get(ObjLvl.TRACK);
            }
            if (properties.containsKey(ObjLvl.POINT)) {
                return properties.get(ObjLvl.POINT);
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
        return new SegmentedTrack();
    }
}
