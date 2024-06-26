/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.scripting;

import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfoList;
import io.github.classgraph.ScanResult;
import io.github.pastorgl.datacooker.RegisteredPackages;
import io.github.pastorgl.datacooker.metadata.OperationMeta;

import java.lang.reflect.Modifier;
import java.util.*;

public class Operations {
    public final static Map<String, OperationInfo> OPERATIONS;

    static {
        Map<String, OperationInfo> operations = new TreeMap<>();

        for (Map.Entry<String, String> pkg : RegisteredPackages.REGISTERED_PACKAGES.entrySet()) {
            try (ScanResult scanResult = new ClassGraph().acceptPackages(pkg.getKey()).scan()) {
                ClassInfoList operationClasses = scanResult.getSubclasses(Operation.class.getTypeName());
                List<Class<?>> operationClassRefs = operationClasses.loadClasses();

                for (Class<?> opClass : operationClassRefs) {
                    try {
                        if (!Modifier.isAbstract(opClass.getModifiers())) {
                            Operation op = (Operation) opClass.getDeclaredConstructor().newInstance();
                            OperationMeta meta = op.meta();
                            String verb = meta.verb;
                            operations.put(verb, new OperationInfo((Class<Operation>) opClass, meta));
                        }
                    } catch (Exception e) {
                        System.err.println("Cannot instantiate Operation class '" + opClass.getTypeName() + "'");
                        e.printStackTrace(System.err);
                        System.exit(8);
                    }
                }
            }
        }

        if (operations.isEmpty()) {
            System.err.println("There are no available Operations in the classpath. Won't continue");
            System.exit(8);
        }

        OPERATIONS = Collections.unmodifiableMap(operations);
    }

    public static Map<String, OperationInfo> packageOperations(String pkgName) {
        Map<String, OperationInfo> ret = new LinkedHashMap<>();

        for (Map.Entry<String, OperationInfo> e : OPERATIONS.entrySet()) {
            if (e.getValue().configurable.getPackage().getName().startsWith(pkgName)) {
                ret.put(e.getKey(), e.getValue());
            }
        }

        return ret;
    }
}
