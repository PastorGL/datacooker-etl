/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.storage;

import io.github.pastorgl.datacooker.data.DataStream;
import io.github.pastorgl.datacooker.data.Partitioning;
import io.github.pastorgl.datacooker.metadata.InputAdapterMeta;
import org.apache.commons.collections4.map.ListOrderedMap;

public abstract class InputAdapter extends StorageAdapter<InputAdapterMeta> {
    public abstract ListOrderedMap<String, DataStream> load(String prefix, int partCount, Partitioning partitioning) throws Exception;
}
