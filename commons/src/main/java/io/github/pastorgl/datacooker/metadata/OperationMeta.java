/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;

import java.util.Map;

public class OperationMeta extends ConfigurableMeta {
    public final DataStreamsMeta input;
    public final DataStreamsMeta output;

    @JsonCreator
    public OperationMeta(String verb, String descr, DataStreamsMeta input, Map<String, DefinitionMeta> definitions, DataStreamsMeta output) {
        super(verb, descr, definitions);

        this.input = input;
        this.output = output;
    }
}
