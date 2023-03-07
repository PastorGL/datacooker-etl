/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.storage;

import io.github.pastorgl.datacooker.data.DataStream;

import java.util.Map;

public abstract class InputAdapter extends StorageAdapter {
    public abstract Map<String, DataStream> load() throws Exception;
}
