/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons.transform;

import io.github.pastorgl.datacooker.commons.transform.functions.PassthruConverter;
import io.github.pastorgl.datacooker.scripting.operation.StreamTransformer;
import io.github.pastorgl.datacooker.data.StreamType;
import io.github.pastorgl.datacooker.scripting.operation.Transformer;
import io.github.pastorgl.datacooker.metadata.PluggableMeta;
import io.github.pastorgl.datacooker.metadata.PluggableMetaBuilder;

import static io.github.pastorgl.datacooker.data.ObjLvl.*;

@SuppressWarnings("unused")
public class PassthruTransform extends Transformer {

    static final String VERB = "passthru";

    @Override
    public PluggableMeta meta() {
        return new PluggableMetaBuilder(VERB,
                "Doesn't change a DataStream in any way")
                .transform().objLvls(VALUE, POINT, POLYGON, TRACK, SEGMENT).operation()
                .input(StreamType.of(StreamType.Passthru), "Input DS")
                .input(StreamType.of(StreamType.Passthru), "Output DS")
                .build();
    }

    @Override
    protected StreamTransformer transformer() {
        return new PassthruConverter(VERB, outputName);
    }
}
