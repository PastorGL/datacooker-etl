/**
 * Copyright (C) 2025 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.metadata;

import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfoList;
import io.github.classgraph.ScanResult;
import io.github.pastorgl.datacooker.RegisteredPackages;
import io.github.pastorgl.datacooker.scripting.operation.TransformCaller;

import java.lang.reflect.Modifier;
import java.util.*;

public class Pluggables {
    public final static Map<String, PluggableInfo> OPERATIONS;
    public final static Map<String, PluggableInfo> TRANSFORMS;
    public final static Map<String, PluggableInfo> INPUTS;
    public final static Map<String, PluggableInfo> OUTPUTS;

    static {
        Map<String, PluggableInfo> inputs = new TreeMap<>();
        Map<String, PluggableInfo> outputs = new TreeMap<>();
        Map<String, PluggableInfo> transforms = new TreeMap<>();
        Map<String, PluggableInfo> operations = new TreeMap<>();

        for (Map.Entry<String, String> pkg : RegisteredPackages.REGISTERED_PACKAGES.entrySet()) {
            try (ScanResult scanResult = new ClassGraph().acceptPackages(pkg.getKey()).scan()) {
                ClassInfoList pluggableClasses = scanResult.getSubclasses(Pluggable.class.getTypeName());
                List<Class<?>> classRefs = pluggableClasses.loadClasses();

                for (Class<?> pClass : classRefs) {
                    try {
                        if (!Modifier.isAbstract(pClass.getModifiers())) {
                            PluggableMeta meta = ((Pluggable<?, ?>) pClass.getDeclaredConstructor().newInstance()).meta();

                            PluggableInfo pi = new PluggableInfo(meta, (Class<Pluggable<?, ?>>) pClass);
                            if (meta.execFlag(ExecFlag.INPUT)) {
                                inputs.put(meta.verb, pi);
                            } else if (meta.execFlag(ExecFlag.OUTPUT)) {
                                outputs.put(meta.verb, pi);
                            } else if (meta.execFlag(ExecFlag.TRANSFORM)) {
                                transforms.put(meta.verb, pi);
                                if (meta.execFlag(ExecFlag.OPERATION)) {
                                    operations.put(meta.verb, new PluggableInfo(meta, (Class) new TransformCaller() {
                                        @Override
                                        public PluggableMeta meta() {
                                            return meta;
                                        }
                                    }.getClass()));
                                }
                            } else {
                                operations.put(meta.verb, pi);
                            }
                        }
                    } catch (Exception e) {
                        System.err.println("Cannot instantiate Pluggable class '" + pClass.getTypeName() + "'");
                        e.printStackTrace(System.err);
                        System.exit(8);
                    }
                }
            }
        }

        if (inputs.isEmpty()) {
            System.err.println("There are no available Input Adapters in the classpath. Won't continue");
            System.exit(5);
        }
        if (outputs.isEmpty()) {
            System.err.println("There are no available Output Adapters in the classpath. Won't continue");
            System.exit(6);
        }
        if (transforms.isEmpty()) {
            System.err.println("There are no available Transforms in the classpath. Won't continue");
            System.exit(7);
        }
        if (operations.isEmpty()) {
            System.err.println("There are no available Operations in the classpath. Won't continue");
            System.exit(8);
        }

        INPUTS = Collections.unmodifiableMap(inputs);
        OUTPUTS = Collections.unmodifiableMap(outputs);
        TRANSFORMS = Collections.unmodifiableMap(transforms);
        OPERATIONS = Collections.unmodifiableMap(operations);
    }

    public static Map<String, PluggableInfo> packageOperations(String pkgName) {
        return packagePluggables(pkgName, OPERATIONS);
    }

    public static Map<String, PluggableInfo> packageTransforms(String pkgName) {
        return packagePluggables(pkgName, TRANSFORMS);
    }

    public static Map<String, PluggableInfo> packageInputs(String pkgName) {
        return packagePluggables(pkgName, INPUTS);
    }

    public static Map<String, PluggableInfo> packageOutputs(String pkgName) {
        return packagePluggables(pkgName, OUTPUTS);
    }

    private static Map<String, PluggableInfo> packagePluggables(String pkgName, Map<String, PluggableInfo> collection) {
        Map<String, PluggableInfo> ret = new LinkedHashMap<>();

        for (Map.Entry<String, PluggableInfo> e : collection.entrySet()) {
            if (e.getValue().pClass.getPackage().getName().startsWith(pkgName)) {
                ret.put(e.getKey(), e.getValue());
            }
        }

        return ret;
    }
}
