/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker;

import io.github.classgraph.AnnotationInfo;
import io.github.classgraph.ClassGraph;
import io.github.classgraph.ScanResult;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

public class RegisteredPackages {
    public static final Map<String, io.github.pastorgl.datacooker.PackageInfo> REGISTERED_PACKAGES;

    static {
        Map<String, io.github.pastorgl.datacooker.PackageInfo> packages = new TreeMap<>();

        try (ScanResult scanResult = new ClassGraph().enableAnnotationInfo().scan()) {
            for (io.github.classgraph.PackageInfo pi : scanResult.getPackageInfo()) {
                AnnotationInfo ai = pi.getAnnotationInfo(RegisteredPackage.class.getCanonicalName());
                if (ai != null) {
                    packages.put(pi.getName(), new io.github.pastorgl.datacooker.PackageInfo(ai.getParameterValues().getValue("value").toString()));
                }
            }
        }

        if (packages.isEmpty()) {
            System.err.println("There are no available Registered Packages in the classpath. Won't continue");
            System.exit(3);
        }

        REGISTERED_PACKAGES = Collections.unmodifiableMap(packages);
    }
}
