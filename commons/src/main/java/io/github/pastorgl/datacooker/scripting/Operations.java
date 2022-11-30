/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.scripting;

import io.github.pastorgl.datacooker.RegisteredPackages;
import io.github.pastorgl.datacooker.metadata.OperationMeta;
import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfoList;
import io.github.classgraph.ScanResult;

import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Operations {
    public final static Map<String, OperationInfo> OPERATIONS;
    public final static Map<String, String> PACKAGES;

    static {
        Map<String, OperationInfo> operations = new HashMap<>();
        Map<String, String> packages = new HashMap<>();

        for (Map.Entry<String, String> pkg : RegisteredPackages.REGISTERED_PACKAGES.entrySet()) {
            try (ScanResult scanResult = new ClassGraph().acceptPackages(pkg.getKey()).scan()) {
                ClassInfoList operationClasses = scanResult.getSubclasses(Operation.class.getTypeName());
                List<Class<?>> operationClassRefs = operationClasses.loadClasses();

                for (Class<?> opClass : operationClassRefs) {
                    try {
                        if (!Modifier.isAbstract(opClass.getModifiers())) {
                            Operation op = (Operation) opClass.newInstance();
                            OperationMeta meta = op.meta();
                            String verb = meta.verb;
                            operations.put(verb, new OperationInfo((Class<? extends Operation>) opClass, meta));
                        }
                    } catch (Exception e) {
                        System.err.println("Cannot instantiate Operation class '" + opClass.getTypeName() + "'");
                        e.printStackTrace(System.err);
                        System.exit(-8);
                    }
                }

                if (!operationClassRefs.isEmpty()) {
                    packages.put(pkg.getKey(), pkg.getValue());
                }
            }
        }

        if (operations.size() == 0) {
            System.err.println("There are no available Operations in the classpath. Won't continue");
            System.exit(-8);
        }

        OPERATIONS = Collections.unmodifiableMap(operations);
        PACKAGES = Collections.unmodifiableMap(packages);
    }

    public static Map<String, OperationInfo> packageOperations(String pkgName) {
        Map<String, OperationInfo> ret = new HashMap<>();

        for (Map.Entry<String, OperationInfo> e : OPERATIONS.entrySet()) {
            if (e.getValue().opClass.getPackage().getName().equals(pkgName)) {
                ret.put(e.getKey(), e.getValue());
            }
        }

        return ret;
    }
}
