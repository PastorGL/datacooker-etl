/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.metadata;

import java.util.Map;

public class OperationMeta {
    public final String verb;
    public final String descr;

    public final DataStreamsMeta input;
    public final DataStreamsMeta output;

    public final Map<String, DefinitionMeta> definitions;

    public OperationMeta(String verb, String descr, DataStreamsMeta input, Map<String, DefinitionMeta> definitions, DataStreamsMeta output) {
        this.verb = verb;
        this.descr = descr;

        this.input = input;
        this.output = output;

        this.definitions = definitions;
    }
}
