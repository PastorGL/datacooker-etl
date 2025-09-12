/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.storage.hadoop.input.functions;

import io.github.pastorgl.datacooker.data.DataRecord;
import io.github.pastorgl.datacooker.data.Partitioning;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public abstract class InputFunction implements Serializable {
    final protected String confXml;
    protected Partitioning partitioning;

    public InputFunction(String hadoopConf, Partitioning partitioning) {
        this.confXml = hadoopConf;
        this.partitioning = partitioning;
    }

    public PairFlatMapFunction<List<String>, Object, DataRecord<?>> build() {
        final Partitioning _partitioning = partitioning;

        return (src) -> {
            List<Tuple2<Object, DataRecord<?>>> ret = new ArrayList<>();

            Random random = new Random();
            try {
                for (String inputFile : src) {
                    RecordInputStream inputStream = recordStream(inputFile);

                    do {
                        DataRecord<?> rec = inputStream.ensureRecord();
                        if (rec == null) {
                            break;
                        } else {
                            Object key = switch (_partitioning) {
                                case RANDOM -> random.nextInt();
                                case SOURCE -> inputFile.hashCode();
                                case HASHCODE -> rec.hashCode();
                            };

                            ret.add(new Tuple2<>(key, rec));
                        }
                    } while (true);

                    inputStream.close();
                }
            } catch (Exception e) {
                throw new RuntimeException("Exception while reading records", e);
            }

            return ret.iterator();
        };
    }

    protected abstract RecordInputStream recordStream(String inputFile) throws Exception;
}
