package io.github.pastorgl.datacooker.storage.hadoop.functions;

import io.github.pastorgl.datacooker.data.Partitioning;
import org.apache.hadoop.conf.Configuration;

public class StructuredInputFunction extends InputFunction {
    public StructuredInputFunction(Partitioning partitioning) {
        super(partitioning);
    }

    @Override
    protected RecordStream recordStream(Configuration conf, String inputFile) throws Exception {
        return null;
    }
}
