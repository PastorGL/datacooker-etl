/**
 * Copyright (C) 2025 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.metadata;

import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfoList;
import io.github.classgraph.ScanResult;
import io.github.pastorgl.datacooker.PackageInfo;
import io.github.pastorgl.datacooker.RegisteredPackages;

import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class Pluggables {
    public final static Map<String, PluggableInfo> PLUGGABLES;
    public final static Map<String, PluggableInfo> INPUTS;
    public final static Map<String, PluggableInfo> OUTPUTS;
    public final static Map<String, PluggableInfo> TRANSFORMS;
    public final static Map<String, PluggableInfo> OPERATIONS;

    static {
        Map<String, PluggableInfo> allPluggables = new TreeMap<>();
        Map<String, PluggableInfo> allInputs = new TreeMap<>();
        Map<String, PluggableInfo> allOutputs = new TreeMap<>();
        Map<String, PluggableInfo> allTransforms = new TreeMap<>();
        Map<String, PluggableInfo> allOperations = new TreeMap<>();

        for (Map.Entry<String, PackageInfo> pkg : RegisteredPackages.REGISTERED_PACKAGES.entrySet()) {
            Map<String, PluggableInfo> pluggables = new TreeMap<>();

            try (ScanResult scanResult = new ClassGraph().acceptPackages(pkg.getKey()).scan()) {
                ClassInfoList pluggableClasses = scanResult.getSubclasses(Pluggable.class.getTypeName());
                List<Class<?>> classRefs = pluggableClasses.loadClasses();

                for (Class<?> pClass : classRefs) {
                    try {
                        if (!Modifier.isAbstract(pClass.getModifiers())) {
                            PluggableMeta meta = ((Pluggable<?, ?>) pClass.getDeclaredConstructor().newInstance()).meta();

                            PluggableInfo pi = new PluggableInfo(meta, (Class<Pluggable<?, ?>>) pClass);
                            pluggables.put(meta.verb, pi);

                            if (meta.execFlag(ExecFlag.INPUT)) {
                                allInputs.put(meta.verb, pi);
                            }
                            if (meta.execFlag(ExecFlag.OUTPUT)) {
                                allOutputs.put(meta.verb, pi);
                            }
                            if (meta.execFlag(ExecFlag.TRANSFORM)) {
                                allTransforms.put(meta.verb, pi);
                            }
                            if (meta.execFlag(ExecFlag.OPERATION)) {
                                allOperations.put(meta.verb, pi);
                            }
                        }
                    } catch (Exception e) {
                        System.err.println("Cannot instantiate Pluggable class '" + pClass.getTypeName() + "'");
                        e.printStackTrace(System.err);
                        System.exit(8);
                    }
                }
            }

            pkg.getValue().pluggables.putAll(pluggables);

            allPluggables.putAll(pluggables);
        }

        if (allInputs.isEmpty()) {
            System.err.println("There are no available Input Adapters in the classpath. Won't continue");
            System.exit(5);
        }
        if (allOutputs.isEmpty()) {
            System.err.println("There are no available Output Adapters in the classpath. Won't continue");
            System.exit(6);
        }
        if (allTransforms.isEmpty()) {
            System.err.println("There are no available Transforms in the classpath. Won't continue");
            System.exit(7);
        }
        if (allOperations.isEmpty()) {
            System.err.println("There are no available Operations in the classpath. Won't continue");
            System.exit(8);
        }

        PLUGGABLES = Collections.unmodifiableMap(allPluggables);
        INPUTS = Collections.unmodifiableMap(allInputs);
        OUTPUTS = Collections.unmodifiableMap(allOutputs);
        TRANSFORMS = Collections.unmodifiableMap(allTransforms);
        OPERATIONS = Collections.unmodifiableMap(allOperations);
    }
}
