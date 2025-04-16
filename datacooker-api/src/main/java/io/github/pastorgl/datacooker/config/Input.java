/**
 * Copyright (C) 2025 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.config;

import io.github.pastorgl.datacooker.data.DataStream;

public class Input implements InputOutput {
    public final DataStream dataStream;

    public Input(DataStream dataStream) {
        this.dataStream = dataStream;
    }
}
