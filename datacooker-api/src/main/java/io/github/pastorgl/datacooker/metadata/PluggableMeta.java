/**
 * Copyright (C) 2025 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import io.github.pastorgl.datacooker.data.ObjLvl;

import java.io.Serializable;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Map;

public class PluggableMeta implements Serializable {
    public final String verb;
    public final String descr;

    public final Map<String, DefinitionMeta> definitions;

    public final InputOutputMeta input;
    public final InputOutputMeta output;

    public final BitSet execFlags;

    public final boolean reqObjLvls;
    public final BitSet objLvls;

    @JsonCreator
    public PluggableMeta(String verb, String descr, InputOutputMeta input, Map<String, DefinitionMeta> definitions, InputOutputMeta output, BitSet execFlags, boolean reqObjLvls, BitSet objLvls) {
        this.verb = verb;
        this.descr = descr;

        this.definitions = definitions;

        this.input = input;
        this.output = output;

        this.execFlags = execFlags;

        this.reqObjLvls = reqObjLvls;
        this.objLvls = objLvls;
    }

    public String[] kind() {
        return Arrays.stream(ExecFlag.values()).filter(ef -> execFlags.get(ef.ordinal())).map(ExecFlag::toString).toArray(String[]::new);
    }

    public boolean execFlag(ExecFlag flag) {
        return execFlags.get(flag.ordinal());
    }

    public ObjLvl[] objLvls() {
        if ((objLvls == null) || objLvls.isEmpty()) {
            return null;
        }

        return Arrays.stream(ObjLvl.values()).filter(ol -> objLvls.get(ol.ordinal())).toArray(ObjLvl[]::new);
    }
}
