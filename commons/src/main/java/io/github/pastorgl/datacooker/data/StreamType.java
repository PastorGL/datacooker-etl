/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.data;

import io.github.pastorgl.datacooker.data.spatial.PointEx;
import io.github.pastorgl.datacooker.data.spatial.PolygonEx;
import io.github.pastorgl.datacooker.data.spatial.SegmentedTrack;

import java.util.List;
import java.util.Map;

public enum StreamType {
    PlainText {
        @Override
        public Accessor accessor(Map<String, List<String>> ignored) {
            return new PlainTextAccessor();
        }

        @Override
        public DataRecord<?> itemTemplate() {
            return new PlainText(new byte[0]);
        }
    },
    Columnar {
        @Override
        public Accessor accessor(Map<String, List<String>> columnNames) {
            return new ColumnarAccessor(columnNames);
        }

        @Override
        public DataRecord<?> itemTemplate() {
            return new Columnar();
        }
    },
    Structured {
        @Override
        public Accessor accessor(Map<String, List<String>> propNames) {
            return new StructuredAccessor(propNames);
        }

        @Override
        public DataRecord<?> itemTemplate() {
            return new Structured();
        }
    },
    Point {
        @Override
        public Accessor accessor(Map<String, List<String>> propNames) {
            return new PointAccessor(propNames);
        }

        @Override
        public DataRecord<?> itemTemplate() {
            return new PointEx();
        }
    },
    Track {
        @Override
        public Accessor accessor(Map<String, List<String>> propNames) {
            return new TrackAccessor(propNames);
        }

        @Override
        public DataRecord<?> itemTemplate() {
            return new SegmentedTrack();
        }
    },
    Polygon {
        @Override
        public Accessor accessor(Map<String, List<String>> propNames) {
            return new PolygonAccessor(propNames);
        }

        @Override
        public DataRecord<?> itemTemplate() {
            return new PolygonEx();
        }
    },
    Passthru {
        @Override
        public Accessor accessor(Map<String, List<String>> propNames) {
            throw new RuntimeException("Attribute accessor of Passthru type DataStream must never be called");
        }

        @Override
        public DataRecord<?> itemTemplate() {
            throw new RuntimeException("Passthru type DataStream item template must never be called");
        }
    };

    public static final StreamType[] EVERY = new StreamType[]{PlainText, Columnar, Structured, Point, Track, Polygon};
    public static final StreamType[] SPATIAL = new StreamType[]{Point, Track, Polygon};
    public static final StreamType[] SIGNAL = new StreamType[]{Columnar, Structured, Point};
    public static final StreamType[] ATTRIBUTED = new StreamType[]{Columnar, Structured, Point, Track, Polygon};

    public abstract Accessor accessor(Map<String, List<String>> propNames);

    public abstract DataRecord<?> itemTemplate();
}
