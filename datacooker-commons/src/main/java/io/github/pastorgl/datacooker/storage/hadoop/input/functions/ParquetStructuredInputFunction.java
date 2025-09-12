/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.storage.hadoop.input.functions;

import io.github.pastorgl.datacooker.data.Partitioning;

public class ParquetStructuredInputFunction extends InputFunction {
    protected String[] _attrs;

    public ParquetStructuredInputFunction(String[] topLevelAttrs, Partitioning partitioning) {
        super(null, partitioning);

        this._attrs = topLevelAttrs;
    }

    @Override
    protected RecordInputStream recordStream(String inputFile) throws Exception {
        return new ParquetStructuredInputStream(inputFile, _attrs);
    }
}
