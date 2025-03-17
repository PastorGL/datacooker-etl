/**
 * Copyright (C) 2024 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.scripting;

import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfoList;
import io.github.classgraph.ScanResult;
import io.github.pastorgl.datacooker.PackageInfo;
import io.github.pastorgl.datacooker.RegisteredPackages;
import io.github.pastorgl.datacooker.metadata.FunctionInfo;

import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class Functions {
    public final static Map<String, FunctionInfo> FUNCTIONS;

    static {
        Map<String, FunctionInfo> allFunctions = new TreeMap<>();

        for (Map.Entry<String, PackageInfo> pkg : RegisteredPackages.REGISTERED_PACKAGES.entrySet()) {
            Map<String, FunctionInfo> functions = new TreeMap<>();

            try (ScanResult scanResult = new ClassGraph().acceptPackages(pkg.getKey()).scan()) {
                ClassInfoList functionClasses = scanResult.getSubclasses(Function.class.getTypeName());
                List<Class<?>> functionClassRefs = functionClasses.loadClasses();

                for (Class<?> funcClass : functionClassRefs) {
                    try {
                        if (!Modifier.isAbstract(funcClass.getModifiers())) {
                            Function<?> func = (Function<?>) funcClass.getDeclaredConstructor().newInstance();
                            functions.put(func.name(), new FunctionInfo(func));
                        }
                    } catch (Exception e) {
                        System.err.println("Cannot instantiate Function class '" + funcClass.getTypeName() + "'");
                        e.printStackTrace(System.err);
                    }
                }
            }

            pkg.getValue().functions.putAll(functions);
            allFunctions.putAll(functions);
        }

        FUNCTIONS = Collections.unmodifiableMap(allFunctions);
    }
}
