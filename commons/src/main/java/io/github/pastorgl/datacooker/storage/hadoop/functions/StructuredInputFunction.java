package io.github.pastorgl.datacooker.storage.hadoop.functions;

import org.apache.hadoop.conf.Configuration;

public class StructuredInputFunction extends InputFunction {
    @Override
    protected RecordStream recordStream(Configuration conf, String inputFile) throws Exception {
        return null;
    }
}
