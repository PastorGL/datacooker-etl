/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.metadata;

import java.io.Serializable;
import java.util.Map;

public class DefinitionMeta implements Serializable {
    public final String descr;

    public final String type;
    public final String friendlyType;

    public final Object defaults;
    public final String defDescr;

    public final Map<String, String> enumValues;

    public final boolean optional;
    public final boolean dynamic;

    DefinitionMeta(String descr, String type, String friendlyType, Object defaults, String defDescr, Map<String, String> enumValues, boolean optional, boolean dynamic) {
        this.descr = descr;

        this.type = type;
        this.friendlyType = friendlyType;

        this.defaults = defaults;
        this.defDescr = defDescr;

        this.enumValues = enumValues;

        this.optional = optional;
        this.dynamic = dynamic;
    }

    public String getType() {
        return friendlyType;
    }
}
