/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.s3direct.functions;

import io.github.pastorgl.datacooker.data.PlainText;
import io.github.pastorgl.datacooker.storage.hadoop.input.functions.RecordInputStream;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

public class PlainTextStream implements RecordInputStream {
    private final BufferedReader reader;

    public PlainTextStream(InputStream inputStream) {
        this.reader = new BufferedReader(new InputStreamReader(inputStream));
    }

    @Override
    public PlainText ensureRecord() throws IOException {
        String line = reader.readLine();

        if (line == null) {
            return null;
        }

        return new PlainText(line.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void close() throws Exception {
        reader.close();
    }
}
