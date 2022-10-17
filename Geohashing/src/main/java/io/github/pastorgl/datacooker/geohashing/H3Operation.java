/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.geohashing;

import io.github.pastorgl.datacooker.metadata.DefinitionMetaBuilder;
import io.github.pastorgl.datacooker.metadata.OperationMeta;
import io.github.pastorgl.datacooker.metadata.Origin;
import io.github.pastorgl.datacooker.metadata.PositionalStreamsMetaBuilder;
import io.github.pastorgl.datacooker.data.StreamType;
import io.github.pastorgl.datacooker.geohashing.functions.H3Function;
import io.github.pastorgl.datacooker.geohashing.functions.HasherFunction;

@SuppressWarnings("unused")
public class H3Operation extends GeohashingOperation {
    private static final Integer DEF_HASH_LEVEL = 9;

    @Override
    public OperationMeta meta() {
        return new OperationMeta("h3", "For each input row with a coordinate pair, generate" +
                " Uber H3 hash with a selected level",

                new PositionalStreamsMetaBuilder()
                        .input("Columnar or Point DataStream with coordinates to hash",
                                new StreamType[]{StreamType.Columnar, StreamType.Point}
                        )
                        .build(),

                new DefinitionMetaBuilder()
                        .def(LAT_COLUMN, "For a Columnar DataStream only, column with latitude, degrees",
                                DEF_CENTER_LAT, "By default, '" + DEF_CENTER_LAT + "'")
                        .def(LON_COLUMN, "For a Columnar DataStream only, column with longitude, degrees",
                                DEF_CENTER_LON, "By default, '" + DEF_CENTER_LON + "'")
                        .def(HASH_LEVEL, "Level of the hash", Integer.class,
                                getDefaultLevel() + "", "Default hash level")
                        .build(),

                new PositionalStreamsMetaBuilder()
                        .output("DataStream with hashed with coordinates",
                                new StreamType[]{StreamType.Columnar, StreamType.Point}, Origin.AUGMENTED, null
                        )
                        .generated(GEN_HASH, "Property with a generated H3 hash hexadecimal string")
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
