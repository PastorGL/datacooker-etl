/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.data;

import io.github.pastorgl.datacooker.metadata.Configurable;
import io.github.pastorgl.datacooker.metadata.TransformMeta;

public interface Transform extends Configurable<TransformMeta> {
    StreamConverter converter();
}
