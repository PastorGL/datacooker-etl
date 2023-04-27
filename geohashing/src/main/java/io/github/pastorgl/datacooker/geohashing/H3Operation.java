/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.geohashing;

import io.github.pastorgl.datacooker.data.StreamType;
import io.github.pastorgl.datacooker.geohashing.functions.H3Function;
import io.github.pastorgl.datacooker.geohashing.functions.HasherFunction;
import io.github.pastorgl.datacooker.metadata.DefinitionMetaBuilder;
import io.github.pastorgl.datacooker.metadata.OperationMeta;
import io.github.pastorgl.datacooker.metadata.Origin;
import io.github.pastorgl.datacooker.metadata.PositionalStreamsMetaBuilder;

@SuppressWarnings("unused")
public class H3Operation extends GeohashingOperation {
    private static final Integer DEF_HASH_LEVEL = 9;

    @Override
    public OperationMeta meta() {
        return new OperationMeta("h3", "Generate a Uber H3 hash with a selected level for each input record",

                new PositionalStreamsMetaBuilder()
                        .input("Columnar or Point DataStream with coordinates",
                                new StreamType[]{StreamType.Columnar, StreamType.Structured, StreamType.Point, StreamType.Polygon, StreamType.Track}
                        )
                        .build(),

                new DefinitionMetaBuilder()
                        .def(LAT_ATTR, "For a Columnar DataStream only, column with latitude, degrees",
                                DEF_CENTER_LAT, "By default, '" + DEF_CENTER_LAT + "'")
                        .def(LON_ATTR, "For a Columnar DataStream only, column with longitude, degrees",
                                DEF_CENTER_LON, "By default, '" + DEF_CENTER_LON + "'")
                        .def(HASH_LEVEL, "Level of the hash", Integer.class,
                                getDefaultLevel() + "", "Default hash level")
                        .build(),

                new PositionalStreamsMetaBuilder()
                        .output("DataStream with hashed coordinates",
                                new StreamType[]{StreamType.Columnar, StreamType.Structured, StreamType.Point, StreamType.Polygon, StreamType.Track},
                                Origin.AUGMENTED, null
                        )
                        .generated(GEN_HASH, "Property with a generated H3 hash as a hexadecimal string")
                        .build()
        );
    }

    @Override
    protected int getMinLevel() {
        return 0;
    }

    @Override
    protected int getMaxLevel() {
        return 15;
    }

    @Override
    protected Integer getDefaultLevel() {
        return DEF_HASH_LEVEL;
    }

    @Override
    protected HasherFunction getHasher() {
        return new H3Function(level);
    }
}
