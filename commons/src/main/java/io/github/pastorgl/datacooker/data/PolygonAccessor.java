/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.data;

import io.github.pastorgl.datacooker.data.spatial.PolygonEx;
import org.apache.commons.collections4.map.SingletonMap;

import java.util.List;
import java.util.Map;

import static io.github.pastorgl.datacooker.Constants.OBJLVL_POLYGON;
import static io.github.pastorgl.datacooker.Constants.OBJLVL_VALUE;

public class PolygonAccessor extends SpatialAccessor<PolygonEx> {
    public PolygonAccessor(Map<String, List<String>> properties) {
        if (properties.containsKey(OBJLVL_VALUE)) {
            this.properties = new SingletonMap<>(OBJLVL_POLYGON, properties.get(OBJLVL_VALUE));
        }
        if (properties.containsKey(OBJLVL_POLYGON)) {
            this.properties = new SingletonMap<>(OBJLVL_POLYGON, properties.get(OBJLVL_POLYGON));
        }
    }
}
