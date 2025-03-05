/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons.transform;

import io.github.pastorgl.datacooker.commons.transform.functions.PassthruConverter;
import io.github.pastorgl.datacooker.data.StreamConverter;
import io.github.pastorgl.datacooker.data.StreamType;
import io.github.pastorgl.datacooker.data.Transform;
import io.github.pastorgl.datacooker.metadata.PluggableMeta;
import io.github.pastorgl.datacooker.metadata.PluggableMetaBuilder;

import static io.github.pastorgl.datacooker.data.ObjLvl.*;

@SuppressWarnings("unused")
public class PassthruTransform extends Transform {
    @Override
    public PluggableMeta initMeta() {
        return new PluggableMetaBuilder("passthru",
                "Doesn't change a DataStream in any way")
                .transform().objLvls(VALUE, POINT, POLYGON, TRACK, SEGMENT).operation()
                .input(StreamType.of(StreamType.Passthru), "Input DS")
                .input(StreamType.of(StreamType.Passthru), "Output DS")
                .build();
    }

    @Override
    public StreamConverter converter() {
        return new PassthruConverter(meta.verb);
    }
}
