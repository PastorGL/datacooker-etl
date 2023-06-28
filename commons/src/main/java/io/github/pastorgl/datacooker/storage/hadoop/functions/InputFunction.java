/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.storage.hadoop.functions;

import io.github.pastorgl.datacooker.data.Partitioning;
import io.github.pastorgl.datacooker.data.Record;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public abstract class InputFunction implements Serializable {
    protected Partitioning partitioning;

    public InputFunction(Partitioning partitioning) {
        this.partitioning = partitioning;
    }

    public PairFlatMapFunction<List<String>, Object, Record<?>> build() {
        final Partitioning _partitioning = partitioning;

        return (src) -> {
            List<Tuple2<Object, Record<?>>> ret = new ArrayList<>();

            Configuration conf = new Configuration();
            Random random = new Random();
            try {
                for (String inputFile : src) {
                    RecordStream inputStream = recordStream(conf, inputFile);

                    do {
                        Record<?> rec = inputStream.ensureRecord();
                        if (rec == null) {
                            break;
                        } else {
                            Object key;
                            switch (_partitioning) {
                                case RANDOM: {
                                    key = random.nextInt();
                                    break;
                                }
                                case SOURCE: {
                                    key = inputFile.hashCode();
                                    break;
                                }
                                default: {
                                    key = rec.hashCode();
                                }
                            }

                            ret.add(new Tuple2<>(key, rec));
                        }
                    } while (true);

                    inputStream.close();
                }
            } catch (Exception e) {
                System.err.println("Exception while reading records: " + e.getMessage());
                e.printStackTrace(System.err);
                System.exit(14);
            }

            return ret.iterator();
        };
    }

    protected abstract RecordStream recordStream(Configuration conf, String inputFile) throws Exception;
}
