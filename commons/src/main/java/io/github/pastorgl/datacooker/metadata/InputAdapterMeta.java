package io.github.pastorgl.datacooker.metadata;

import io.github.pastorgl.datacooker.data.StreamType;
import io.github.pastorgl.datacooker.metadata.AdapterMeta;
import io.github.pastorgl.datacooker.metadata.DefinitionMeta;

import java.util.Map;

public class InputAdapterMeta extends AdapterMeta {
    public InputAdapterMeta(String verb, String descr, String pathDescr, StreamType type, Map<String, DefinitionMeta> meta) {
        super(verb, descr, pathDescr, new StreamType[]{type}, meta);
    }
}
