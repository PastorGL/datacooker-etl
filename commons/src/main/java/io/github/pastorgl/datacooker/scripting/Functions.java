/**
 * Copyright (C) 2024 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.scripting;

import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfoList;
import io.github.classgraph.ScanResult;
import io.github.pastorgl.datacooker.RegisteredPackages;

import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Functions {
    public final static Map<String, Function> FUNCTIONS;

    static {
        Map<String, Function> functions = new HashMap<>();

        for (Map.Entry<String, String> pkg : RegisteredPackages.REGISTERED_PACKAGES.entrySet()) {
            try (ScanResult scanResult = new ClassGraph().acceptPackages(pkg.getKey()).scan()) {
                ClassInfoList functionClasses = scanResult.getSubclasses(Function.class.getTypeName());
                List<Class<?>> functionClassRefs = functionClasses.loadClasses();

                for (Class<?> funcClass : functionClassRefs) {
                    try {
                        if (!Modifier.isAbstract(funcClass.getModifiers())) {
                            Function func = (Function) funcClass.getDeclaredConstructor().newInstance();
                            functions.put(func.name(), func);
                        }
                    } catch (Exception e) {
                        System.err.println("Cannot instantiate Function class '" + funcClass.getTypeName() + "'");
                        e.printStackTrace(System.err);
                    }
                }
            }
        }

        FUNCTIONS = Collections.unmodifiableMap(functions);
    }

    public static Function get(String text) {
        return FUNCTIONS.get(text);
    }
}
