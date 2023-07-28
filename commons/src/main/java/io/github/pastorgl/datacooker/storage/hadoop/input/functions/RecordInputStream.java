/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.storage.hadoop.input.functions;

import io.github.pastorgl.datacooker.data.Record;

import java.io.IOException;

public interface RecordInputStream extends AutoCloseable {
    Record<?> ensureRecord() throws IOException;
}
