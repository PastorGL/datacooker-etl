/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.metadata;

import io.github.pastorgl.datacooker.data.StreamType;

import java.util.Map;

public class OutputAdapterMeta extends AdapterMeta{
    public OutputAdapterMeta(String verb, String descr, String pathDescr, StreamType[] type, Map<String, DefinitionMeta> meta) {
        super(verb, descr, pathDescr, type, meta);
    }
}
