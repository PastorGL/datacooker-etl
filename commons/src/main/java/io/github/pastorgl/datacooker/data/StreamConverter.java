/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.data;

import io.github.pastorgl.datacooker.scripting.ParamsContext;

import java.util.List;
import java.util.Map;

@FunctionalInterface
public interface StreamConverter {
    DataStream apply(DataStream ds, Map<String, List<String>> newColumns, ParamsContext params);
}
