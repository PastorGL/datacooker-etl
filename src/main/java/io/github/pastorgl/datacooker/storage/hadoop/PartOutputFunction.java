/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.storage.hadoop;

import io.github.pastorgl.datacooker.data.BinRec;
import com.opencsv.CSVWriter;
import org.apache.commons.collections4.map.ListOrderedMap;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.apache.spark.api.java.function.Function2;

import java.io.OutputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;

public class PartOutputFunction implements Function2<Integer, Iterator<BinRec>, Iterator<Void>> {
    protected final String _name;
    protected final String outputPath;
    protected final HadoopStorage.Codec codec;
    protected final String[] _columns;
    protected final char _delimiter;

    public PartOutputFunction(String _name, String outputPath, HadoopStorage.Codec codec, String[] _columns, char _delimiter) {
        this.outputPath = outputPath;
        this.codec = codec;
        this._columns = _columns;
        this._name = _name;
        this._delimiter = _delimiter;
    }

    @Override
    public Iterator<Void> call(Integer idx, Iterator<BinRec> it) {
        Configuration conf = new Configuration();

        try {
            writePart(conf, idx, it);
        } catch (Exception e) {
            System.err.println("Exception while writing records: " + e.getMessage());
            e.printStackTrace(System.err);
            System.exit(15);
        }

        return Collections.emptyIterator();
    }

    protected void writePart(Configuration conf, int idx, Iterator<BinRec> it) throws Exception {
        String suffix = HadoopStorage.suffix(outputPath);

        String partName = (_name.isEmpty() ? "" : ("/" + _name)) + "/" + String.format("part-%05d", idx);

        if ("parquet".equalsIgnoreCase(suffix)) {
            partName = outputPath.substring(0, outputPath.lastIndexOf(".")) + partName
                    + ((codec != HadoopStorage.Codec.NONE) ? "." + codec.name().toLowerCase() : "") + ".parquet";

            Path partPath = new Path(partName);
            FileSystem outputFs = partPath.getFileSystem(conf);
            outputFs.setVerifyChecksum(false);
            outputFs.setWriteChecksum(false);

            writeToParquetFile(conf, partPath, it);
        } else {
            partName = outputPath + partName
                    + ((codec != HadoopStorage.Codec.NONE) ? "." + codec.name().toLowerCase() : "");

            Path partPath = new Path(partName);

            FileSystem outputFs = partPath.getFileSystem(conf);
            outputFs.setVerifyChecksum(false);
            outputFs.setWriteChecksum(false);
            OutputStream outputStream = outputFs.create(partPath);

            writeToTextFile(conf, outputStream, it);
        }
    }

    protected void writeToParquetFile(Configuration conf, Path partPath, Iterator<BinRec> it) throws Exception {
        boolean first = true;
        ParquetWriter<Group> writer = null;
        MessageType schema = null;
        String[] columns = null;
        while (it.hasNext()) {
            BinRec line = it.next();

            if (first) {
                List<Type> types = new ArrayList<>();
                if (_columns == null) {
                    ListOrderedMap<String, Object> map = line.asIs();
                    columns = map.keyList().toArray(new String[0]);
                } else {
                    columns = _columns;
                }
                for (String col : columns) {
                    types.add(Types.primitive(BINARY, Type.Repetition.REQUIRED).as(stringType()).named(col));
                }
                schema = new MessageType(_name, types);

                ExampleParquetWriter.Builder builder = ExampleParquetWriter.builder(partPath)
                        .withConf(conf)
                        .withType(schema)
                        .withPageWriteChecksumEnabled(false);

                if (codec != HadoopStorage.Codec.NONE) {
                    builder.withCompressionCodec(CompressionCodecName.fromCompressionCodec(codec.codec));
                }
                writer = builder.build();

                first = false;
            }

            Group group = new SimpleGroup(schema);

            for (String col : columns) {
                group.add(col, line.asString(col));
            }

            writer.write(group);
        }

        if (writer != null) {
            writer.close();
        }
    }

    protected void writeToTextFile(Configuration conf, OutputStream outputStream, Iterator<BinRec> it) throws Exception {
        if (codec != HadoopStorage.Codec.NONE) {
            Class<? extends CompressionCodec> cc = codec.codec;
            CompressionCodec codec = cc.getDeclaredConstructor().newInstance();
            ((Configurable) codec).setConf(conf);

            outputStream = codec.createOutputStream(outputStream);
        }

        while (it.hasNext()) {
            BinRec next = it.next();

            StringWriter stringBuffer = new StringWriter();

            ListOrderedMap<String, Object> map = next.asIs();
            if (map.get(0).isEmpty()) {
                stringBuffer.append(String.valueOf(map.getValue(0))).append("\n");
            } else {
                String[] acc;
                if (_columns != null) {
                    acc = new String[_columns.length];
                    for (int i = 0; i < _columns.length; i++) {
                        String col = _columns[i];
                        acc[i] = next.asString(col);
                    }
                } else {
                    int size = map.size();
                    acc = new String[size];
                    for (int i = 0; i < size; i++) {
                        String col = map.get(i);
                        acc[i] = next.asString(col);
                    }
                }

                CSVWriter writer = new CSVWriter(stringBuffer, _delimiter, CSVWriter.DEFAULT_QUOTE_CHARACTER,
                        CSVWriter.DEFAULT_ESCAPE_CHARACTER, CSVWriter.DEFAULT_LINE_END);
                writer.writeNext(acc, false);
                writer.close();
            }

            outputStream.write(stringBuffer.toString().getBytes(StandardCharsets.UTF_8));
        }
    }
}
