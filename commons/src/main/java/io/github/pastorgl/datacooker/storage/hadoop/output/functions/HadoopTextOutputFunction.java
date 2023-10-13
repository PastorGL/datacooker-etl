/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.storage.hadoop.output.functions;

import com.opencsv.CSVWriter;
import io.github.pastorgl.datacooker.data.Record;
import io.github.pastorgl.datacooker.storage.hadoop.HadoopStorage;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import scala.Tuple2;

import java.io.IOException;
import java.io.OutputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;

public class HadoopTextOutputFunction extends OutputFunction {
    protected final String[] columns;
    protected final char delimiter;

    public HadoopTextOutputFunction(String sub, String outputPath, HadoopStorage.Codec codec, String[] columns, char delimiter) {
        super(sub, outputPath, codec);

        this.columns = columns;
        this.delimiter = delimiter;
    }

    protected void writePart(Configuration conf, int idx, Iterator<Tuple2<Object, Record<?>>> it) throws Exception {
        String partName = (sub.isEmpty() ? "" : ("/" + sub)) + "/" + String.format("part-%05d", idx);

        partName = outputPath + partName
                + ((codec != HadoopStorage.Codec.NONE) ? "." + codec.name().toLowerCase() : "");

        Path partPath = new Path(partName);

        FileSystem outputFs = partPath.getFileSystem(conf);
        outputFs.setVerifyChecksum(false);
        outputFs.setWriteChecksum(false);
        OutputStream outputStream = outputFs.create(partPath);

        if (codec != HadoopStorage.Codec.NONE) {
            Class<? extends CompressionCodec> cc = codec.codec;
            CompressionCodec codec = cc.getDeclaredConstructor().newInstance();
            ((Configurable) codec).setConf(conf);

            outputStream = codec.createOutputStream(outputStream);
        }

        System.out.println("Writing Text file " + partPath);
        writeToTextFile(it, outputStream);
    }

    protected void writeToTextFile(Iterator<Tuple2<Object, Record<?>>> it, OutputStream outputStream) throws IOException {
        while (it.hasNext()) {
            Record<?> next = it.next()._2;

            StringWriter stringBuffer = new StringWriter();

            List<String> attrs = next.attrs();
            if (attrs.isEmpty()) {
                stringBuffer.append(next.asString("")).append("\n");
            } else {
                String[] acc;
                if (columns != null) {
                    acc = new String[columns.length];
                    for (int i = 0; i < columns.length; i++) {
                        String col = columns[i];
                        acc[i] = next.asString(col);
                    }
                } else {
                    int size = attrs.size();
                    acc = new String[size];
                    for (int i = 0; i < size; i++) {
                        String col = attrs.get(i);
                        acc[i] = next.asString(col);
                    }
                }

                CSVWriter writer = new CSVWriter(stringBuffer, delimiter, CSVWriter.DEFAULT_QUOTE_CHARACTER,
                        CSVWriter.DEFAULT_ESCAPE_CHARACTER, CSVWriter.DEFAULT_LINE_END);
                writer.writeNext(acc, false);
                writer.close();
            }

            outputStream.write(stringBuffer.toString().getBytes(StandardCharsets.UTF_8));
        }

        outputStream.close();
    }
}
