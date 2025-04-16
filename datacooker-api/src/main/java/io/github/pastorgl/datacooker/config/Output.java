/**
 * Copyright (C) 2025 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.config;

import io.github.pastorgl.datacooker.data.ObjLvl;

import java.util.List;
import java.util.Map;

public class Output implements InputOutput {
    public final String name;
    public final Map<ObjLvl, List<String>> requested;

    public Output(String name, Map<ObjLvl, List<String>> requested) {
        this.name = name;
        this.requested = requested;
    }
}
