/**
 * Copyright (C) 2025 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import io.github.pastorgl.datacooker.data.ObjLvl;

import java.io.Serializable;
import java.util.BitSet;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class PluggableMeta implements Serializable {
    public final String verb;
    public final String descr;

    public final Map<String, DefinitionMeta> definitions;

    public final InputOutputMeta input;
    public final InputOutputMeta output;

    private final BitSet execFlags;
    private final BitSet dsFlags;
    private final BitSet objLvls;

    @JsonCreator
    public PluggableMeta(String verb, String descr, InputOutputMeta input, Map<String, DefinitionMeta> definitions, InputOutputMeta output, BitSet execFlags, BitSet dsFlags, BitSet objLvls) {
        this.verb = verb;
        this.descr = descr;

        this.definitions = definitions;

        this.input = input;
        this.output = output;

        this.execFlags = execFlags;

        this.dsFlags = dsFlags;
        this.objLvls = objLvls;
    }

    public boolean execFlag(ExecFlag flag) {
        return execFlags.get(flag.ordinal());
    }

    public boolean dsFlag(DSFlag flag) {
        if (dsFlags == null) {
            return false;
        }
        return dsFlags.get(flag.ordinal());
    }

    public Set<ObjLvl> objLvls() {
        if (objLvls == null) {
            return null;
        }

        Set<ObjLvl> ret = new HashSet<>();
        for (ObjLvl ol: ObjLvl.values()) {
            if (objLvls.get(ol.ordinal())) {
                ret.add(ol);
            }
        }
        return ret;
    }
}
