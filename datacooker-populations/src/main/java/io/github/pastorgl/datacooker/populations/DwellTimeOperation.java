/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.populations;

import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.config.Configuration;
import io.github.pastorgl.datacooker.data.*;
import io.github.pastorgl.datacooker.metadata.PluggableMeta;
import io.github.pastorgl.datacooker.metadata.PluggableMetaBuilder;
import io.github.pastorgl.datacooker.scripting.operation.MergerOperation;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static io.github.pastorgl.datacooker.data.ObjLvl.VALUE;

@SuppressWarnings("unused")
public class DwellTimeOperation extends MergerOperation {
    static final String RDD_INPUT_TARGET = "target";
    static final String RDD_INPUT_SIGNALS = "signals";

    static final String SIGNALS_USERID_ATTR = "signals_userid_attr";
    static final String TARGET_USERID_ATTR = "target_userid_attr";
    static final String TARGET_GROUPING_ATTR = "target_grouping_attr";

    static final String GEN_DWELLTIME = "_dwelltime";
    static final String VERB = "dwellTime";

    private String signalsUseridAttr;

    private String targetUseridAttr;
    private String targetGroupingAttr;

    @Override
    public PluggableMeta meta() {
        return new PluggableMetaBuilder(VERB, "Statistical indicator for the Dwell Time of a sub-population of users" +
                " that they spend within target group (i.e. grid cell ID)")
                .operation()
                .input(RDD_INPUT_SIGNALS, StreamType.SIGNAL, "Source user signals")
                .input(RDD_INPUT_TARGET, StreamType.SIGNAL, "Target audience signals, a sub-population of base audience signals")
                .def(SIGNALS_USERID_ATTR, "Source DataStream attribute with the user ID")
                .def(TARGET_USERID_ATTR, "Target audience DataStream attribute with the user ID")
                .def(TARGET_GROUPING_ATTR, "Target audience DataStream grouping attribute (i.e. grid cell ID)")
                .output(StreamType.COLUMNAR, "Generated DataStream with Dwell Time indicator for each value of grouping attribute, which is in the key",
                        StreamOrigin.GENERATED, Collections.singletonList(RDD_INPUT_TARGET))
                .generated(GEN_DWELLTIME, "Dwell Time statistical indicator")
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

        // userid -> S
        DataStream inputSignals = inputStreams.get(RDD_INPUT_SIGNALS);
        JavaPairRDD<String, Long> S = inputSignals.rdd()
                .mapPartitionsToPair(it -> {
                    List<Tuple2<String, Void>> ret = new ArrayList<>();

                    while (it.hasNext()) {
                        DataRecord<?> row = it.next()._2;

                        String userid = row.asString(_signalsUseridColumn);

                        ret.add(new Tuple2<>(userid, null));
                    }

                    return ret.iterator();
                })
                .aggregateByKey(0L, (c, v) -> c + 1L, Long::sum);

        String _targetUseridAttr = targetUseridAttr;
        String _targetGroupingAttr = targetGroupingAttr;

        // userid -> groupid, s
        DataStream inputTarget = inputStreams.get(RDD_INPUT_TARGET);
        JavaPairRDD<String, Tuple2<String, Long>> s = inputTarget.rdd()
                .mapPartitionsToPair(it -> {
                    List<Tuple2<Tuple2<String, String>, Void>> ret = new ArrayList<>();
                    while (it.hasNext()) {
                        DataRecord<?> row = it.next()._2;

                        String userid = row.asString(_targetUseridAttr);
                        String groupid = row.asString(_targetGroupingAttr);

                        ret.add(new Tuple2<>(new Tuple2<>(userid, groupid), null));
                    }

                    return ret.iterator();
                })
                .aggregateByKey(0L, (c, v) -> c + 1L, Long::sum)
                .mapToPair(t -> new Tuple2<>(t._1._1, new Tuple2<>(t._1._2, t._2)));

        final List<String> outputColumns = Collections.singletonList(GEN_DWELLTIME);

        JavaPairRDD<Object, DataRecord<?>> output = s.join(S)
                .mapToPair(t -> new Tuple2<>(t._2._1._1, t._2._1._2.doubleValue() / t._2._2.doubleValue()))
                .aggregateByKey(new Tuple2<>(0L, 0.D),
                        (c, t) -> new Tuple2<>(c._1 + 1L, c._2 + t),
                        (c1, c2) -> new Tuple2<>(c1._1 + c2._1, c1._2 + c2._2)
                )
                .mapToPair(c -> new Tuple2<>(c._1, new Columnar(outputColumns, new Object[]{c._2._2 / c._2._1})));

        return new DataStreamBuilder(outputName, Collections.singletonMap(VALUE, outputColumns))
                .generated(VERB, StreamType.Columnar, inputTarget)
                .build(output);
    }
}
