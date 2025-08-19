/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.storage.hadoop.input.functions;

import io.github.pastorgl.datacooker.data.Partitioning;
import org.apache.hadoop.conf.Configuration;

import java.io.ByteArrayInputStream;

public class ParquetColumnarInputFunction extends InputFunction {
    protected String[] _columns;

    public ParquetColumnarInputFunction(String[] columns, String hadoopConf, Partitioning partitioning) {
        super(hadoopConf, partitioning);

        _columns = columns;
    }

    protected RecordInputStream recordStream(String inputFile) throws Exception {
        Configuration hadoopConf = new Configuration();
        hadoopConf.addResource(new ByteArrayInputStream(confXml.getBytes()));

        return new ParquetColumnarInputStream(hadoopConf, inputFile, _columns);
    }
}
