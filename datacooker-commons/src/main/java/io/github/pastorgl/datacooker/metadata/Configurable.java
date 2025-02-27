/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.metadata;

import java.io.Serializable;

public interface Configurable<M extends ConfigurableMeta> extends Serializable {
    M initMeta();
    M meta();
}
