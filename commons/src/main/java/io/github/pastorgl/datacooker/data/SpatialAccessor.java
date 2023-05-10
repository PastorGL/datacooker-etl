/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.data;

import io.github.pastorgl.datacooker.data.spatial.SpatialRecord;
import org.locationtech.jts.geom.Geometry;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.github.pastorgl.datacooker.Constants.*;

public abstract class SpatialAccessor<G extends Geometry & SpatialRecord<G>> implements Accessor<G> {
    protected Map<String, List<String>> properties = new HashMap<>();

    public List<String> attributes(String category) {
        if (OBJLVL_VALUE.equals(category)) {
            if (properties.containsKey(OBJLVL_POLYGON)) {
                return properties.get(OBJLVL_POLYGON);
            }
            if (properties.containsKey(OBJLVL_TRACK)) {
                return properties.get(OBJLVL_TRACK);
            }
            if (properties.containsKey(OBJLVL_POINT)) {
                return properties.get(OBJLVL_POINT);
            }
        }
        return properties.getOrDefault(category, Collections.EMPTY_LIST);
    }

    @Override
    public Map<String, List<String>> attributes() {
        return properties;
    }

    @Override
    public void set(G obj, String attr, Object value) {
        obj.put(attr, value);
    }

    @Override
    public AttrGetter getter(Record<?> obj) {
        Map<String, Object> props = obj.asIs();
        if (props.isEmpty()) {
            return (p) -> null;
        }
        return obj::asIs;
    }
}
