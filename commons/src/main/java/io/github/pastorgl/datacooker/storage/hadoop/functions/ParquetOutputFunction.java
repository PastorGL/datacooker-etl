/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.storage.hadoop.functions;

import io.github.pastorgl.datacooker.data.Columnar;
import io.github.pastorgl.datacooker.data.Record;
import io.github.pastorgl.datacooker.storage.hadoop.HadoopStorage;
import org.apache.commons.collections4.map.ListOrderedMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;

public class ParquetOutputFunction extends PartOutputFunction {
    protected final String[] columns;

    public ParquetOutputFunction(String sub, String outputPath, HadoopStorage.Codec codec, String[] columns) {
        super(sub, outputPath, codec);

        this.columns = columns;
    }

    protected void writePart(Configuration conf, int idx, Iterator<Tuple2<Object, Record<?>>> it) throws Exception {
        String partName = (sub.isEmpty() ? "" : ("/" + sub)) + "/" + String.format("part-%05d", idx);

        partName = outputPath.substring(0, outputPath.lastIndexOf(".")) + partName
                + ((codec != HadoopStorage.Codec.NONE) ? "." + codec.name().toLowerCase() : "") + ".parquet";

        Path partPath = new Path(partName);
        FileSystem outputFs = partPath.getFileSystem(conf);
        outputFs.setVerifyChecksum(false);
        outputFs.setWriteChecksum(false);

        boolean first = true;
        ParquetWriter<Group> writer = null;
        MessageType schema = null;
        String[] columns = null;
        while (it.hasNext()) {
            Record<?> line = it.next()._2;

            if (first) {
                List<Type> types = new ArrayList<>();
                if (this.columns == null) {
                    ListOrderedMap<String, Object> map = ((Columnar) line).asIs();
                    columns = map.keyList().toArray(new String[0]);
                } else {
                    columns = this.columns;
                }
                for (String col : columns) {
                    types.add(Types.primitive(BINARY, Type.Repetition.REQUIRED).as(stringType()).named(col));
                }
                schema = new MessageType(sub, types);

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
}
