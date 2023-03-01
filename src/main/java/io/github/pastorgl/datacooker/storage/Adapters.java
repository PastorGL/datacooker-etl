/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.storage;

import io.github.classgraph.ClassGraph;
import io.github.classgraph.ScanResult;
import io.github.pastorgl.datacooker.RegisteredPackages;
import io.github.pastorgl.datacooker.metadata.AdapterMeta;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Adapters {
    static public final Map<String, AdapterInfo> INPUTS;
    static public final Map<String, AdapterInfo> OUTPUTS;

    static public final Map<String, String> INPUT_PACKAGES;
    static public final Map<String, String> OUTPUT_PACKAGES;

    static {
        Map<String, AdapterInfo> inputs = new HashMap<>();
        Map<String, AdapterInfo> outputs = new HashMap<>();
        Map<String, String> inputPackages = new HashMap<>();
        Map<String, String> outputPackages = new HashMap<>();

        for (Map.Entry<String, String> pkg : RegisteredPackages.REGISTERED_PACKAGES.entrySet()) {
            try (ScanResult scanResult = new ClassGraph().enableClassInfo().acceptPackages(pkg.getKey()).scan()) {
                List<Class<?>> iaClassRefs = scanResult.getSubclasses(InputAdapter.class.getTypeName()).loadClasses();

                for (Class<?> iaClass : iaClassRefs) {
                    try {
                        InputAdapter ia = (InputAdapter) iaClass.getDeclaredConstructor().newInstance();
                        AdapterMeta meta = ia.meta;
                        AdapterInfo ai = new AdapterInfo((Class<StorageAdapter>) iaClass, meta);
                        inputs.put(meta.verb, ai);
                    } catch (Exception e) {
                        System.err.println("Cannot instantiate Input Adapter class '" + iaClass.getTypeName() + "'");
                        e.printStackTrace(System.err);
                        System.exit(-8);
                    }
                }

                if (!iaClassRefs.isEmpty()) {
                    inputPackages.put(pkg.getKey(), pkg.getValue());
                }

                List<Class<?>> oaClassRefs = scanResult.getSubclasses(OutputAdapter.class.getTypeName()).loadClasses();

                for (Class<?> oaClass : oaClassRefs) {
                    try {
                        OutputAdapter oa = (OutputAdapter) oaClass.getDeclaredConstructor().newInstance();
                        AdapterMeta meta = oa.meta;
                        AdapterInfo ai = new AdapterInfo((Class<StorageAdapter>) oaClass, meta);
                        outputs.put(meta.verb, ai);
                    } catch (Exception e) {
                        System.err.println("Cannot instantiate Output Adapter class '" + oaClass.getTypeName() + "'");
                        e.printStackTrace(System.err);
                        System.exit(-8);
                    }
                }

                if (!oaClassRefs.isEmpty()) {
                    outputPackages.put(pkg.getKey(), pkg.getValue());
                }
            }
        }

        INPUTS = Collections.unmodifiableMap(inputs);
        OUTPUTS = Collections.unmodifiableMap(outputs);
        INPUT_PACKAGES = Collections.unmodifiableMap(inputPackages);
        OUTPUT_PACKAGES = Collections.unmodifiableMap(outputPackages);
    }

    static public InputAdapter inputAdapter(String name) throws Exception {
        for (AdapterInfo ia : INPUTS.values()) {
            if (name.matches(ia.meta.verb)) {
                return (InputAdapter) ia.configurable.getDeclaredConstructor().newInstance();
            }
        }

        return null;
    }

    static public OutputAdapter outputAdapter(String name) throws Exception {
        for (AdapterInfo oa : OUTPUTS.values()) {
            if (name.matches(oa.meta.verb)) {
                return (OutputAdapter) oa.configurable.getDeclaredConstructor().newInstance();
            }
        }

        return null;
    }

    public static Map<String, AdapterInfo> packageInputs(String pkgName) {
        Map<String, AdapterInfo> ret = new HashMap<>();

        for (Map.Entry<String, AdapterInfo> e : INPUTS.entrySet()) {
            if (e.getValue().configurable.getPackage().getName().equals(pkgName)) {
                ret.put(e.getKey(), e.getValue());
            }
        }

        return ret;
    }

    public static Map<String, AdapterInfo> packageOutputs(String pkgName) {
        Map<String, AdapterInfo> ret = new HashMap<>();

        for (Map.Entry<String, AdapterInfo> e : OUTPUTS.entrySet()) {
            if (e.getValue().configurable.getPackage().getName().equals(pkgName)) {
                ret.put(e.getKey(), e.getValue());
            }
        }

        return ret;
    }
}
