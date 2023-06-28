/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker;

import io.github.classgraph.AnnotationInfo;
import io.github.classgraph.ClassGraph;
import io.github.classgraph.PackageInfo;
import io.github.classgraph.ScanResult;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class RegisteredPackages {
    public static final Map<String, String> REGISTERED_PACKAGES;

    static {
        HashMap<String, String> packages = new HashMap<>();

        try (ScanResult scanResult = new ClassGraph().enableAnnotationInfo().scan()) {
            for (PackageInfo pi : scanResult.getPackageInfo()) {
                AnnotationInfo ai = pi.getAnnotationInfo(RegisteredPackage.class.getCanonicalName());
                if (ai != null) {
                    packages.put(pi.getName(), ai.getParameterValues().getValue("value").toString());
                }
            }
        }

        REGISTERED_PACKAGES = Collections.unmodifiableMap(packages);
    }
}
