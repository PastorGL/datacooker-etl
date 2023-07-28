/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.data;

import io.github.pastorgl.datacooker.data.spatial.SegmentedTrack;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.github.pastorgl.datacooker.Constants.*;

public class TrackAccessor extends SpatialAccessor<SegmentedTrack> {
    public TrackAccessor(Map<String, List<String>> properties) {
        this.properties = new HashMap<>();
        if (properties.containsKey(OBJLVL_VALUE)) {
            this.properties.put(OBJLVL_TRACK, properties.get(OBJLVL_VALUE));
        }
        if (properties.containsKey(OBJLVL_TRACK)) {
            this.properties.put(OBJLVL_TRACK, properties.get(OBJLVL_TRACK));
        }
        if (properties.containsKey(OBJLVL_SEGMENT)) {
            this.properties.put(OBJLVL_SEGMENT, properties.get(OBJLVL_SEGMENT));
        }
        if (properties.containsKey(OBJLVL_POINT)) {
            this.properties.put(OBJLVL_POINT, properties.get(OBJLVL_POINT));
        }
    }
}
