/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.populations;

import io.github.pastorgl.datacooker.config.Configuration;
import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.data.*;
import io.github.pastorgl.datacooker.metadata.PluggableMeta;
import io.github.pastorgl.datacooker.metadata.PluggableMetaBuilder;
import io.github.pastorgl.datacooker.scripting.operation.MergerOperation;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.util.*;

import static io.github.pastorgl.datacooker.data.ObjLvl.VALUE;

@SuppressWarnings("unused")
public class ReachOperation extends MergerOperation {
    public static final String RDD_INPUT_TARGET = "target";
    public static final String RDD_INPUT_SIGNALS = "signals";

    static final String SIGNALS_USERID_ATTR = "signals_userid_attr";
    static final String TARGET_USERID_ATTR = "target_userid_attr";
    static final String TARGET_GROUPING_ATTR = "target_grouping_attr";

    static final String GEN_REACH = "_reach";
    static final String VERB = "reach";

    private String signalsUseridAttr;

    private String targetUseridAttr;
    private String targetGroupingAttr;

    @Override
    public PluggableMeta meta() {
        return new PluggableMetaBuilder(VERB, "Statistical indicator for some target audience Reach of source population," +
                " selected by grouping attribute (i.e. grid cell ID)")
                .operation()
                .input(RDD_INPUT_SIGNALS, StreamType.SIGNAL, "Source user signals")
                .input(RDD_INPUT_TARGET, StreamType.SIGNAL, "Target audience signals, a sub-population of base audience signals")
                .def(SIGNALS_USERID_ATTR, "Source DataStream attribute with the user ID")
                .def(TARGET_USERID_ATTR, "Target audience DataStream attribute with the user ID")
                .def(TARGET_GROUPING_ATTR, "Target audience DataStream grouping attribute")
                .output(StreamType.COLUMNAR, "Generated DataStream with Reach indicator for each value of grouping attribute, which is in the key",
                        StreamOrigin.GENERATED, Collections.singletonList(RDD_INPUT_TARGET))
                .generated(GEN_REACH, "Reach statistical indicator")
                .build();

    }

    @Override
    public void configure(Configuration params) throws InvalidConfigurationException {
        signalsUseridAttr = params.get(SIGNALS_USERID_ATTR);

        targetUseridAttr = params.get(TARGET_USERID_ATTR);
        targetGroupingAttr = params.get(TARGET_GROUPING_ATTR);
    }

    @Override
    public DataStream merge() {
        String _signalsUseridColumn = signalsUseridAttr;

        final long N = inputStreams.get(RDD_INPUT_SIGNALS).rdd()
                .mapPartitions(it -> {
                    List<String> ret = new ArrayList<>();

                    while (it.hasNext()) {
                        DataRecord<?> row = it.next()._2;

                        String userid = row.asString(_signalsUseridColumn);

                        ret.add(userid);
                    }

                    return ret.iterator();
                })
                .distinct()
                .count();

        String _targetUseridAttr = targetUseridAttr;
        String _targetGroupingAttr = targetGroupingAttr;

        final List<String> outputColumns = Collections.singletonList(GEN_REACH);

        DataStream inputTarget = inputStreams.get(RDD_INPUT_TARGET);
        JavaPairRDD<Object, DataRecord<?>> output = inputTarget.rdd()
                .mapPartitionsToPair(it -> {
                    List<Tuple2<String, String>> ret = new ArrayList<>();
                    while (it.hasNext()) {
                        DataRecord<?> row = it.next()._2;

                        String groupid = row.asString(_targetGroupingAttr);
                        String userid = row.asString(_targetUseridAttr);

                        ret.add(new Tuple2<>(groupid, userid));
                    }

                    return ret.iterator();
                })
                .combineByKey(
                        t1 -> {
                            Set<String> s = new HashSet<>();
                            s.add(t1);
                            return s;
                        },
                        (c, t1) -> {
                            c.add(t1);
                            return c;
                        },
                        (c1, c2) -> {
                            c1.addAll(c2);
                            return c1;
                        }
                )
                .mapToPair(t -> new Tuple2<>(t._1, new Columnar(outputColumns, new Object[]{((double) t._2.size()) / N})));

        return new DataStreamBuilder(outputName, Collections.singletonMap(VALUE, outputColumns))
                .generated(VERB, StreamType.Columnar, inputTarget)
                .build(output);
    }
}
