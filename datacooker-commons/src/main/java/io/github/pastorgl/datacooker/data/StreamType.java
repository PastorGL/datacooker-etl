/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.data;

public enum StreamType {
    PlainText,
    Columnar,
    Structured,
    Point,
    Track,
    Polygon,
    Passthru;

    public static final StreamType[] EVERY = new StreamType[]{PlainText, Columnar, Structured, Point, Track, Polygon};
    public static final StreamType[] SPATIAL = new StreamType[]{Point, Track, Polygon};
    public static final StreamType[] SIGNAL = new StreamType[]{Columnar, Structured, Point};
    public static final StreamType[] ATTRIBUTED = new StreamType[]{Columnar, Structured, Point, Track, Polygon};
}
