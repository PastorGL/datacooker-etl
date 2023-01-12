package io.github.pastorgl.datacooker.storage;

import io.github.pastorgl.datacooker.data.BinRec;

import java.io.IOException;

public interface RecordStream extends AutoCloseable {
    BinRec ensureRecord() throws IOException;
}
