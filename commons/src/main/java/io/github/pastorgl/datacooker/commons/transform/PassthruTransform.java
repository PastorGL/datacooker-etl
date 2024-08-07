/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons.transform;

import io.github.pastorgl.datacooker.data.DataStreamBuilder;
import io.github.pastorgl.datacooker.data.StreamConverter;
import io.github.pastorgl.datacooker.data.StreamType;
import io.github.pastorgl.datacooker.data.Transform;
import io.github.pastorgl.datacooker.metadata.TransformMeta;

@SuppressWarnings("unused")
public class PassthruTransform extends Transform {
    @Override
    public TransformMeta meta() {
        return new TransformMeta("passthru", StreamType.Passthru, StreamType.Passthru,
                "Doesn't change a DataStream in any way",

                null,
                null
        );
    }

    @Override
    public StreamConverter converter() {
        return (ds, newColumns, params) -> new DataStreamBuilder(ds.name, ds.streamType, ds.accessor.attributes())
                .passedthru(meta.verb, ds)
                .build(ds.rdd);
    }
}
