/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.metadata;

import java.util.Map;

public abstract class ConfigurableMeta {
    public final String verb;
    public final String descr;

    public final Map<String, DefinitionMeta> definitions;

    protected ConfigurableMeta(String verb, String descr, Map<String, DefinitionMeta> definitions) {
        this.verb = verb;
        this.descr = descr;

        this.definitions = definitions;
    }
}
