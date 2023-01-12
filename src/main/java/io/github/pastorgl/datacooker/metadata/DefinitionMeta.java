/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.metadata;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Map;

public class DefinitionMeta {
    public final String descr;

    public final String type;
    @JsonIgnore
    public final String hrType;

    public final Object defaults;
    public final String defDescr;

    public final Map<String, String> values;

    public final boolean optional;
    public final boolean dynamic;

    DefinitionMeta(String descr, String type, String hrType, Object defaults, String defDescr, Map<String, String> values, boolean optional, boolean dynamic) {
        this.descr = descr;

        this.type = type;
        this.hrType = hrType;

        this.defaults = defaults;
        this.defDescr = defDescr;

        this.values = values;

        this.optional = optional;
        this.dynamic = dynamic;
    }

    @JsonGetter
    public String getType() {
        return hrType;
    }
}
