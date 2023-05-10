package io.github.pastorgl.datacooker.metadata;

import io.github.pastorgl.datacooker.data.StreamType;

import java.util.Map;

public class OutputAdapterMeta extends AdapterMeta{
    public OutputAdapterMeta(String verb, String descr, String pathDescr, StreamType[] type, Map<String, DefinitionMeta> meta) {
        super(verb, descr, pathDescr, type, meta);
    }
}
