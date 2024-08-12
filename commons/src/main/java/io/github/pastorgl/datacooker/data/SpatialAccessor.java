/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.data;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.github.pastorgl.datacooker.Constants.*;

public abstract class SpatialAccessor implements Accessor {
    protected final Map<String, List<String>> properties = new HashMap<>();

    public List<String> attributes(String objLvl) {
        if (OBJLVL_VALUE.equals(objLvl)) {
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
        return properties.getOrDefault(objLvl, Collections.emptyList());
    }

    @Override
    public Map<String, List<String>> attributes() {
        return properties;
    }
}
