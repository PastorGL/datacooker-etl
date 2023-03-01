/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.storage;

import io.github.pastorgl.datacooker.metadata.AdapterMeta;
import io.github.pastorgl.datacooker.metadata.ConfigurableInfo;

public class AdapterInfo extends ConfigurableInfo<StorageAdapter, AdapterMeta> {
    public AdapterInfo(Class<StorageAdapter> adapterClass, AdapterMeta meta) {
        super(adapterClass, meta);
    }
}
