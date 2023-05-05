/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.storage.hadoop.functions;

import io.github.pastorgl.datacooker.data.Record;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public abstract class InputFunction implements Serializable {
    public InputFunction() {
    }

    public PairFlatMapFunction<List<String>, Object, Record<?>> build() {
        return (src) -> {
            List<Tuple2<Object, Record<?>>> ret = new ArrayList<>();

            Configuration conf = new Configuration();
            try {
                for (String inputFile : src) {
                    RecordStream inputStream = recordStream(conf, inputFile);

                    do {
                        Record<?> rec = inputStream.ensureRecord();
                        if (rec == null) {
                            break;
                        } else {
                            ret.add(new Tuple2<>(rec.hashCode(), rec));
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
