/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.data;

import io.github.pastorgl.datacooker.RegisteredPackages;
import io.github.pastorgl.datacooker.metadata.TransformMeta;
import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfoList;
import io.github.classgraph.ScanResult;

import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings({"unchecked", "rawtypes"})
public class Transforms {
    public final static Map<String, TransformInfo> TRANSFORMS;

    static {
        Map<String, TransformInfo> transforms = new HashMap<>();

        for (Map.Entry<String, String> pkg : RegisteredPackages.REGISTERED_PACKAGES.entrySet()) {
            try (ScanResult scanResult = new ClassGraph().acceptPackages(pkg.getKey()).scan()) {
                ClassInfoList transformClasses = scanResult.getClassesImplementing(Transform.class.getTypeName());
                List<Class<?>> transformClassRefs = transformClasses.loadClasses();

                for (Class<?> transformClass : transformClassRefs) {
                    try {
                        if (!Modifier.isAbstract(transformClass.getModifiers())) {
                            Transform transform = (Transform) transformClass.newInstance();
                            TransformMeta meta = transform.meta();
                            String verb = meta.verb;
                            transforms.put(verb, new TransformInfo((Class<? extends Transform>) transformClass, meta));
                        }
                    } catch (Exception e) {
                        System.err.println("Cannot instantiate Transform class '" + transformClass.getTypeName() + "'");
                        e.printStackTrace(System.err);
                        System.exit(-8);
                    }
                }
            }
        }

        if (transforms.size() == 0) {
            System.err.println("There are no available Transforms in the classpath. Won't continue");
            System.exit(-8);
        }

        TRANSFORMS = Collections.unmodifiableMap(transforms);
    }

    public static Map<String, TransformInfo> packageTransforms(String pkgName) {
        Map<String, TransformInfo> ret = new HashMap<>();

        for (Map.Entry<String, TransformInfo> e : TRANSFORMS.entrySet()) {
            if (e.getValue().transformClass.getPackage().getName().equals(pkgName)) {
                ret.put(e.getKey(), e.getValue());
            }
        }

        return ret;
    }
}
