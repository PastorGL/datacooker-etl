/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.storage;

import io.github.pastorgl.datacooker.data.DataStream;
import io.github.pastorgl.datacooker.data.ObjLvl;

import java.util.List;
import java.util.Map;

public abstract class OutputAdapter extends StorageAdapter {
    public abstract void save(String sub, DataStream rdd, Map<ObjLvl, List<String>> filterColumns);
}
