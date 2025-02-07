/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import io.github.pastorgl.datacooker.data.StreamType.StreamTypes;

import java.util.Map;

public class InputAdapterMeta extends AdapterMeta {
    @JsonCreator
    public InputAdapterMeta(String verb, String descr, String[] paths, StreamTypes type, Map<String, DefinitionMeta> meta) {
        super(verb, descr, paths, type, meta);
    }
}
