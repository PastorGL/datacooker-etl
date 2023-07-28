/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.storage;

import io.github.classgraph.ClassGraph;
import io.github.classgraph.ScanResult;
import io.github.pastorgl.datacooker.RegisteredPackages;
import io.github.pastorgl.datacooker.metadata.InputAdapterMeta;
import io.github.pastorgl.datacooker.metadata.OutputAdapterMeta;

import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Adapters {
    static public final Map<String, InputAdapterInfo> INPUTS;
    static public final Map<String, OutputAdapterInfo> OUTPUTS;

    static {
        Map<String, InputAdapterInfo> inputs = new HashMap<>();
        Map<String, OutputAdapterInfo> outputs = new HashMap<>();

        for (Map.Entry<String, String> pkg : RegisteredPackages.REGISTERED_PACKAGES.entrySet()) {
            try (ScanResult scanResult = new ClassGraph().enableClassInfo().acceptPackages(pkg.getKey()).scan()) {
                List<Class<?>> iaClassRefs = scanResult.getSubclasses(InputAdapter.class.getTypeName()).loadClasses();

                for (Class<?> iaClass : iaClassRefs) {
                    try {
                        if (!Modifier.isAbstract(iaClass.getModifiers())) {
                            InputAdapter ia = (InputAdapter) iaClass.getDeclaredConstructor().newInstance();
                            InputAdapterMeta meta = ia.meta;
                            InputAdapterInfo ai = new InputAdapterInfo((Class<InputAdapter>) iaClass, meta);
                            inputs.put(meta.verb, ai);
                        }
                    } catch (Exception e) {
                        System.err.println("Cannot instantiate Input Adapter class '" + iaClass.getTypeName() + "'");
                        e.printStackTrace(System.err);
                        System.exit(5);
                    }
                }

                List<Class<?>> oaClassRefs = scanResult.getSubclasses(OutputAdapter.class.getTypeName()).loadClasses();

                for (Class<?> oaClass : oaClassRefs) {
                    try {
                        if (!Modifier.isAbstract(oaClass.getModifiers())) {
                            OutputAdapter oa = (OutputAdapter) oaClass.getDeclaredConstructor().newInstance();
                            OutputAdapterMeta meta = oa.meta;
                            OutputAdapterInfo ai = new OutputAdapterInfo((Class<OutputAdapter>) oaClass, meta);
                            outputs.put(meta.verb, ai);
                        }
                    } catch (Exception e) {
                        System.err.println("Cannot instantiate Output Adapter class '" + oaClass.getTypeName() + "'");
                        e.printStackTrace(System.err);
                        System.exit(6);
                    }
                }
            }
        }

        if (inputs.size() == 0) {
            System.err.println("There are no available Input Adapters in the classpath. Won't continue");
            System.exit(5);
        }
        if (outputs.size() == 0) {
            System.err.println("There are no available Output Adapters in the classpath. Won't continue");
            System.exit(6);
        }

        INPUTS = Collections.unmodifiableMap(inputs);
        OUTPUTS = Collections.unmodifiableMap(outputs);
    }

    public static Map<String, InputAdapterInfo> packageInputs(String pkgName) {
        Map<String, InputAdapterInfo> ret = new HashMap<>();

        for (Map.Entry<String, InputAdapterInfo> e : INPUTS.entrySet()) {
            if (e.getValue().configurable.getPackage().getName().equals(pkgName)) {
                ret.put(e.getKey(), e.getValue());
            }
        }

        return ret;
    }

    public static Map<String, OutputAdapterInfo> packageOutputs(String pkgName) {
        Map<String, OutputAdapterInfo> ret = new HashMap<>();

        for (Map.Entry<String, OutputAdapterInfo> e : OUTPUTS.entrySet()) {
            if (e.getValue().configurable.getPackage().getName().equals(pkgName)) {
                ret.put(e.getKey(), e.getValue());
            }
        }

        return ret;
    }
}
