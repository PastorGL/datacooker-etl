/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.storage.hadoop.input.functions;

import io.github.pastorgl.datacooker.data.Partitioning;
import org.apache.hadoop.conf.Configuration;

public class StructuredInputFunction extends InputFunction {
    public StructuredInputFunction(Partitioning partitioning) {
        super(partitioning);
    }

    @Override
    protected RecordInputStream recordStream(Configuration conf, String inputFile) throws Exception {
        return null;
    }
}
