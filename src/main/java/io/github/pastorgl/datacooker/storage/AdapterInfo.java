/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.storage;

import io.github.pastorgl.datacooker.metadata.AdapterMeta;

public class AdapterInfo {
    public final Class<? extends StorageAdapter> adapterClass;
    public final AdapterMeta meta;

    public AdapterInfo(Class<? extends StorageAdapter> adapterClass, AdapterMeta meta) {
        this.adapterClass = adapterClass;
        this.meta = meta;
    }
}
