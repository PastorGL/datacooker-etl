/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.metadata;

import io.github.pastorgl.datacooker.data.StreamType;
import io.github.pastorgl.datacooker.metadata.AdapterMeta;
import io.github.pastorgl.datacooker.metadata.DefinitionMeta;

import java.util.Map;

public class InputAdapterMeta extends AdapterMeta {
    public InputAdapterMeta(String verb, String descr, String[] paths, StreamType type, Map<String, DefinitionMeta> meta) {
        super(verb, descr, paths, new StreamType[]{type}, meta);
    }
}
