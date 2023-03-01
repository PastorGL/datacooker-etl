/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.metadata;

import java.util.Map;

public class AdapterMeta extends ConfigurableMeta {
    public AdapterMeta(String verb, String descr, Map<String, DefinitionMeta> definitions) {
        super(verb, descr, definitions);
    }
}
