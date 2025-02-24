/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons.transform;

import io.github.pastorgl.datacooker.commons.transform.functions.PassthruConverter;
import io.github.pastorgl.datacooker.data.*;
import io.github.pastorgl.datacooker.metadata.TransformMeta;

@SuppressWarnings("unused")
public class PassthruTransform extends Transform {
    @Override
    public TransformMeta initMeta() {
        return new TransformMeta("passthru", StreamType.Passthru, StreamType.Passthru,
                "Doesn't change a DataStream in any way except optional top-level attribute filtering",

                null,
                null,
                true
        );
    }

    @Override
    public StreamConverter converter() {
        return new PassthruConverter(meta.verb);
    }
}
