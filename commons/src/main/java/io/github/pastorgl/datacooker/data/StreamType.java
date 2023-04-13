/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.data;

import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.data.spatial.PointEx;
import io.github.pastorgl.datacooker.data.spatial.PolygonEx;
import io.github.pastorgl.datacooker.data.spatial.SegmentedTrack;

import java.util.List;
import java.util.Map;

public enum StreamType {
    PlainText {
        @Override
        public Accessor<PlainText> accessor(Map<String, List<String>> ignored) {
            return new PlainTextAccessor();
        }
    },
    Columnar {
        @Override
        public Accessor<Columnar> accessor(Map<String, List<String>> columnNames) {
            return new ColumnarAccessor(columnNames);
        }
    },
    KeyValue {
        @Override
        public Accessor<Columnar> accessor(Map<String, List<String>> columnNames) {
            return new ColumnarAccessor(columnNames);
        }
    },
    Structured {
        @Override
        public Accessor<?> accessor(Map<String, List<String>> propNames) {
            return new StructuredAccessor(propNames);
        }
    },
    Point {
        @Override
        public Accessor<PointEx> accessor(Map<String, List<String>> propNames) {
            return new PointAccessor(propNames);
        }
    },
    Track {
        @Override
        public Accessor<SegmentedTrack> accessor(Map<String, List<String>> propNames) {
            return new TrackAccessor(propNames);
        }
    },
    Polygon {
        @Override
        public Accessor<PolygonEx> accessor(Map<String, List<String>> propNames) {
            return new PolygonAccessor(propNames);
        }
    },
    Passthru {
        @Override
        public Accessor<?> accessor(Map<String, List<String>> propNames) {
            throw new InvalidConfigurationException("Attribute accessor of Passthru type DataStream must never be called");
        }
    };

    public abstract Accessor<?> accessor(Map<String, List<String>> propNames);
}
