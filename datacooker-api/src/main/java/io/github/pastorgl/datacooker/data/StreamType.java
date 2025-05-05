/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

public enum StreamType {
    PlainText,
    Columnar,
    Structured,
    Point,
    Track,
    Polygon,
    Passthru;

    public static class StreamTypes implements Serializable {
        @JsonValue
        public final StreamType[] types;

        @JsonCreator
        StreamTypes(StreamType... types) {
            this.types = types;
        }

        @Override
        public String toString() {
            return Arrays.stream(types).map(Enum::name).collect(Collectors.joining(", "));
        }
    }

    public static final StreamTypes PLAIN_TEXT = new StreamTypes(PlainText);
    public static final StreamTypes COLUMNAR = new StreamTypes(Columnar);
    public static final StreamTypes STRUCTURED = new StreamTypes(Structured);
    public static final StreamTypes POINT = new StreamTypes(Point);
    public static final StreamTypes POLYGON = new StreamTypes(Polygon);
    public static final StreamTypes TRACK = new StreamTypes(Track);
    public static final StreamTypes EVERY = new StreamTypes(PlainText, Columnar, Structured, Point, Track, Polygon);
    public static final StreamTypes SPATIAL = new StreamTypes(Point, Track, Polygon);
    public static final StreamTypes SIGNAL = new StreamTypes(Columnar, Structured, Point);
    public static final StreamTypes ATTRIBUTED = new StreamTypes(Columnar, Structured, Point, Track, Polygon);

    private static final StreamTypes[] predefined = new StreamTypes[]{PLAIN_TEXT, COLUMNAR, STRUCTURED, POINT, POLYGON,
            TRACK, EVERY, SPATIAL, SIGNAL, ATTRIBUTED};

    public static StreamTypes of(StreamType... types) {
        for (StreamTypes pre : predefined) {
            Set<StreamType> preSet = Set.of(pre.types);
            Set<StreamType> typeSet = Set.of(types);

            if (preSet.equals(typeSet)) {
                return pre;
            }
        }

        return new StreamTypes(types);
    }

    public ObjLvl topLevel() {
        return switch (this) {
            case Point -> ObjLvl.POINT;
            case Track -> ObjLvl.TRACK;
            case Polygon -> ObjLvl.POLYGON;
            default -> ObjLvl.VALUE;
        };
    }
}
