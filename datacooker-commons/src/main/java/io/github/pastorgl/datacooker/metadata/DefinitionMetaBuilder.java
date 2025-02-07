/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.metadata;

import java.util.HashMap;
import java.util.Map;

public class DefinitionMetaBuilder {
    private final Map<String, DefinitionMeta> defs;

    public DefinitionMetaBuilder() {
        defs = new HashMap<>();
    }

    public DefinitionMetaBuilder def(String name, String descr, Class<?> type, Object defaults, String defDescr) {
        defs.put(name, getMeta(descr, type, defaults, defDescr, true, false));

        return this;
    }

    public DefinitionMetaBuilder def(String name, String descr, String defaults, String defDescr) {
        defs.put(name, getMeta(descr, String.class, defaults, defDescr, true, false));

        return this;
    }

    public DefinitionMetaBuilder def(String name, String descr) {
        defs.put(name, getMeta(descr, String.class, null, null, false, false));

        return this;
    }

    public DefinitionMetaBuilder def(String name, String descr, Class<?> type) {
        defs.put(name, getMeta(descr, type, null, null, false, false));

        return this;
    }

    public DefinitionMetaBuilder dynDef(String name, String descr, Class<?> type) {
        defs.put(name, getMeta(descr, type, null, null, true, true));

        return this;
    }

    private DefinitionMeta getMeta(String descr, Class<?> type, Object defaults, String defDescr, boolean optional, boolean dynamic) {
        String typeName;

        String canonicalName = type.getCanonicalName();
        if (type.isArray()) {
            typeName = "[L" + canonicalName.substring(0, canonicalName.length() - 2) + ";";
        } else if (type.isMemberClass()) {
            int lastDot = canonicalName.lastIndexOf('.');
            typeName = canonicalName.substring(0, lastDot) + "$" + canonicalName.substring(lastDot + 1);
        } else {
            typeName = canonicalName;
        }

        Map<String, String> values = null;
        if (type.isEnum()) {
            values = new HashMap<>();
            for (DescribedEnum e : type.asSubclass(DescribedEnum.class).getEnumConstants()) {
                values.put(e.name(), e.descr());
            }
        }

        return new DefinitionMeta(descr, typeName, type.getSimpleName(), defaults, defDescr, values, optional, dynamic);
    }

    public Map<String, DefinitionMeta> build() {
        return defs;
    }
}
