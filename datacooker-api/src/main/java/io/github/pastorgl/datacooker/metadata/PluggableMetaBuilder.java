/**
 * Copyright (C) 2025 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.metadata;

import io.github.pastorgl.datacooker.data.ObjLvl;
import io.github.pastorgl.datacooker.data.StreamOrigin;
import io.github.pastorgl.datacooker.data.StreamType;

import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PluggableMetaBuilder {
    private final String verb;
    private final String descr;

    private final Map<String, DefinitionMeta> defs;

    private InputOutputMeta input;
    private InputOutputMeta output;

    private final BitSet execFlags;
    private BitSet dsFlags;
    private BitSet objLvls;

    public PluggableMetaBuilder(String verb, String descr) {
        this.verb = verb;
        this.descr = descr;

        this.defs = new HashMap<>();
        this.execFlags = new BitSet();
    }

    public PluggableMetaBuilder operation() {
        this.execFlags.set(ExecFlag.OPERATION.ordinal());
        return this;
    }

    public PluggableMetaBuilder transform() {
        this.execFlags.set(ExecFlag.TRANSFORM.ordinal());
        this.dsFlags = new BitSet();
        return this;
    }

    public PluggableMetaBuilder transform(boolean keyBefore) {
        this.execFlags.set(ExecFlag.TRANSFORM.ordinal());
        this.dsFlags = new BitSet();
        if (keyBefore) {
            this.dsFlags.set(DSFlag.KEY_BEFORE.ordinal());
        }
        return this;
    }

    public PluggableMetaBuilder inputAdapter(String[] paths) {
        this.execFlags.set(ExecFlag.INPUT.ordinal());
        this.input = new PathMeta(paths, false);
        this.dsFlags = new BitSet();
        return this;
    }

    public PluggableMetaBuilder inputAdapter(String[] paths, boolean wildcard) {
        this.execFlags.set(ExecFlag.INPUT.ordinal());
        this.input = new PathMeta(paths, wildcard);
        this.dsFlags = new BitSet();
        return this;
    }

    public PluggableMetaBuilder outputAdapter(String[] paths) {
        this.execFlags.set(ExecFlag.OUTPUT.ordinal());
        this.output = new PathMeta(paths, false);
        this.dsFlags = new BitSet();
        return this;
    }

    public PluggableMetaBuilder input(StreamType.StreamTypes type, String descr) {
        this.input = new InputMeta(type, descr);
        return this;
    }

    public PluggableMetaBuilder output(StreamType.StreamTypes type, String descr) {
        this.output = new OutputMeta(type, descr, false);
        return this;
    }

    public PluggableMetaBuilder output(StreamType.StreamTypes type, String descr, StreamOrigin origin, List<String> ancestors) {
        this.output = new OutputMeta(type, descr, false, origin, ancestors);
        return this;
    }

    public PluggableMetaBuilder input(String name, StreamType.StreamTypes type, String descr) {
        if (this.input == null) {
            this.input = new NamedInputMeta();
        }
        ((NamedInputMeta) this.input).streams.put(name, new InputMeta(type, descr, false));
        return this;
    }

    public PluggableMetaBuilder optInput(String name, StreamType.StreamTypes type, String descr) {
        if (this.input == null) {
            this.input = new NamedInputMeta();
        }
        ((NamedInputMeta) this.input).streams.put(name, new InputMeta(type, descr, true));
        return this;
    }

    public PluggableMetaBuilder output(String name, StreamType.StreamTypes type, String descr) {
        if (this.output == null) {
            this.output = new NamedOutputMeta();
        }
        ((NamedOutputMeta) this.output).streams.put(name, new OutputMeta(type, descr, false));
        return this;
    }

    public PluggableMetaBuilder output(String name, StreamType.StreamTypes type, String descr, StreamOrigin origin, List<String> ancestors) {
        if (this.output == null) {
            this.output = new NamedOutputMeta();
        }
        ((NamedOutputMeta) this.output).streams.put(name, new OutputMeta(type, descr, false, origin, ancestors));
        return this;
    }

    public PluggableMetaBuilder optOutput(String name, StreamType.StreamTypes type, String descr, StreamOrigin origin, List<String> ancestors) {
        if (this.output == null) {
            this.output = new NamedOutputMeta();
        }
        ((NamedOutputMeta) this.output).streams.put(name, new OutputMeta(type, descr, true, origin, ancestors));
        return this;
    }

    public PluggableMetaBuilder generated(String propName, String propDescr) {
        ((OutputMeta) this.output).generated.put(propName, propDescr);
        return this;
    }

    public PluggableMetaBuilder generated(String name, String propName, String propDescr) {
        ((NamedOutputMeta) this.output).streams.get(name).generated.put(propName, propDescr);
        return this;
    }

    public PluggableMetaBuilder reqObjLvls(ObjLvl... objLvls) {
        this.dsFlags.set(DSFlag.REQUIRES_OBJLVL.ordinal());

        return objLvls(objLvls);
    }

    public PluggableMetaBuilder objLvls(ObjLvl... objLvls) {
        if (objLvls != null) {
            if (this.objLvls == null) {
                this.objLvls = new BitSet();
            }
            for (ObjLvl objLvl : objLvls) {
                this.objLvls.set(objLvl.ordinal());
            }
        }

        return this;
    }

    public PluggableMetaBuilder def(String name, String descr, Class<?> type, Object defaults, String defDescr) {
        defs.put(name, makeDef(descr, type, defaults, defDescr, true, false));

        return this;
    }

    public PluggableMetaBuilder def(String name, String descr, String defaults, String defDescr) {
        defs.put(name, makeDef(descr, String.class, defaults, defDescr, true, false));

        return this;
    }

    public PluggableMetaBuilder def(String name, String descr) {
        defs.put(name, makeDef(descr, String.class, null, null, false, false));

        return this;
    }

    public PluggableMetaBuilder def(String name, String descr, Class<?> type) {
        defs.put(name, makeDef(descr, type, null, null, false, false));

        return this;
    }

    public PluggableMetaBuilder dynDef(String name, String descr, Class<?> type) {
        defs.put(name, makeDef(descr, type, null, null, true, true));

        return this;
    }

    private DefinitionMeta makeDef(String descr, Class<?> type, Object defaults, String defDescr, boolean optional, boolean dynamic) {
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

        Map<String, String> enumValues = null;
        if (type.isEnum()) {
            enumValues = new HashMap<>();
            for (DescribedEnum e : type.asSubclass(DescribedEnum.class).getEnumConstants()) {
                enumValues.put(e.name(), e.descr());
            }
        }

        return new DefinitionMeta(descr, typeName, type.getSimpleName(), defaults, defDescr, enumValues, optional, dynamic);
    }

    public PluggableMeta build() {
        return new PluggableMeta(verb, descr, input, defs.isEmpty() ? null : defs, output, execFlags, dsFlags, objLvls);
    }
}
