/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.geohashing;

import io.github.pastorgl.datacooker.data.StreamType;
import io.github.pastorgl.datacooker.geohashing.functions.HasherFunction;
import io.github.pastorgl.datacooker.geohashing.functions.JapanMeshFunction;
import io.github.pastorgl.datacooker.metadata.DefinitionMetaBuilder;
import io.github.pastorgl.datacooker.metadata.OperationMeta;
import io.github.pastorgl.datacooker.metadata.Origin;
import io.github.pastorgl.datacooker.metadata.PositionalStreamsMetaBuilder;

@SuppressWarnings("unused")
public class JapanMeshOperation extends GeohashingOperation {
    private static final Integer DEF_HASH_LEVEL = 6;

    @Override
    public OperationMeta meta() {
        return new OperationMeta("japanMesh", "Generate a Japan Mesh hash with a selected level for each record",

                new PositionalStreamsMetaBuilder()
                        .input("Columnar or Point DataStream with coordinates",
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
                        .output("DataStream with hashed coordinates",
                                new StreamType[]{StreamType.Columnar, StreamType.Point}, Origin.AUGMENTED, null
                        )
                        .generated(GEN_HASH, "Column with a generated Japan Mesh string")
                        .build()
        );
    }

    @Override
    protected int getMinLevel() {
        return 1;
    }

    @Override
    protected int getMaxLevel() {
        return 6;
    }

    @Override
    protected Integer getDefaultLevel() {
        return DEF_HASH_LEVEL;
    }

    @Override
    protected HasherFunction getHasher() {
        return new JapanMeshFunction(level);
    }
}
