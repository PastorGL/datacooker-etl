/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.storage;

import io.github.pastorgl.datacooker.metadata.ConfigurableInfo;
import io.github.pastorgl.datacooker.metadata.InputAdapterMeta;

public class InputAdapterInfo extends ConfigurableInfo<InputAdapter, InputAdapterMeta> {
    public InputAdapterInfo(Class<InputAdapter> adapterClass, InputAdapterMeta meta) {
        super(adapterClass, meta);
    }
}
