/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.storage;

import io.github.pastorgl.datacooker.metadata.ConfigurableInfo;
import io.github.pastorgl.datacooker.metadata.OutputAdapterMeta;

public class OutputAdapterInfo extends ConfigurableInfo<OutputAdapter, OutputAdapterMeta> {
    public OutputAdapterInfo(Class<OutputAdapter> adapterClass, OutputAdapterMeta meta) {
        super(adapterClass, meta);
    }
}
