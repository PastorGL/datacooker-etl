/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.scripting.operation;

import io.github.pastorgl.datacooker.config.Configuration;
import io.github.pastorgl.datacooker.data.DataStream;
import io.github.pastorgl.datacooker.data.ObjLvl;

import java.util.List;
import java.util.Map;

@FunctionalInterface
public interface StreamTransformer {
    DataStream apply(DataStream ds, Map<ObjLvl, List<String>> newColumns, Configuration params);
}
