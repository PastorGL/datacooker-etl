/**
 * Copyright (C) 2025 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.data;

public enum ObjLvl {
    VALUE("Value"),
    POINT("Point"),
    TRACK("SegmentedTrack"),
    SEGMENT("TrackSegment"),
    POLYGON("Polygon");

    private final String friendlyName;

    ObjLvl(String friendlyName) {
        this.friendlyName = friendlyName;
    }

    public static ObjLvl get(String lvl) {
        if (lvl == null) return VALUE;
        switch (lvl.toUpperCase()) {
            case "POI": case "POINT": return POINT;
            case "POLYGON": return POLYGON;
            case "SEGMENT": case "TRACKSEGMENT": return SEGMENT;
            case "SEGMENTEDTRACK": case "TRACK": return TRACK;
            default: return VALUE;
        }
    }

    @Override
    public String toString() {
        return friendlyName;
    }
}
