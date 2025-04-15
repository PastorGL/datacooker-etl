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
        return (lvl == null) ? VALUE : switch (lvl.toUpperCase()) {
            case "POI", "POINT" -> POINT;
            case "POLYGON" -> POLYGON;
            case "SEGMENT", "TRACKSEGMENT" -> SEGMENT;
            case "SEGMENTEDTRACK", "TRACK" -> TRACK;
            default -> VALUE;
        };
    }

    @Override
    public String toString() {
        return friendlyName;
    }
}
