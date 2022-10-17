/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.populations;

import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.data.Columnar;
import io.github.pastorgl.datacooker.data.DataStream;
import io.github.pastorgl.datacooker.data.Record;
import io.github.pastorgl.datacooker.data.StreamType;
import io.github.pastorgl.datacooker.metadata.*;
import io.github.pastorgl.datacooker.scripting.Operation;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static io.github.pastorgl.datacooker.config.Constants.OBJLVL_VALUE;

@SuppressWarnings("unused")
public class DwellTimeOperation extends Operation {
    static final String RDD_INPUT_TARGET = "target";
    static final String RDD_INPUT_SIGNALS = "signals";

    static final String SIGNALS_USERID_ATTR = "signals_userid_attr";
    static final String TARGET_USERID_ATTR = "target_userid_attr";
    static final String TARGET_GROUPING_ATTR = "target_grouping_attr";

    static final String GEN_DWELLTIME = "_dwelltime";

    private String signalsUseridAttr;

    private String targetUseridAttr;
    private String targetGroupingAttr;

    @Override
    public OperationMeta meta() {
        return new OperationMeta("dwellTime", "Statistical indicator for the Dwell Time of a sub-population" +
                " that they spend in target cells",

                new NamedStreamsMetaBuilder()
                        .mandatoryInput(RDD_INPUT_SIGNALS, "Source user signals",
                                new StreamType[]{StreamType.Columnar, StreamType.Point}
                        )
                        .mandatoryInput(RDD_INPUT_TARGET, "Target audience signals, a sub-population of base audience signals",
                                new StreamType[]{StreamType.Columnar, StreamType.Point}
                        )
                        .build(),

                new DefinitionMetaBuilder()
                        .def(SIGNALS_USERID_ATTR, "Source DataStream attribute with the user ID")
                        .def(TARGET_USERID_ATTR, "Target audience DataStream attribute with the user ID")
                        .def(TARGET_GROUPING_ATTR, "Target audience DataStream grouping attribute (i.e. grid cell ID)")
                        .build(),

                new PositionalStreamsMetaBuilder()
                        .output("Generated DataStream with Dwell Time indicator for each value of grouping attribute, which is in the key",
                                new StreamType[]{StreamType.KeyValue}, Origin.GENERATED, Collections.singletonList(RDD_INPUT_TARGET)
                        )
                        .generated(GEN_DWELLTIME, "Dwell time statistical indicator")
                        .build()
        );
    }

    @Override
    public void configure() throws InvalidConfigurationException {
        signalsUseridAttr = params.get(SIGNALS_USERID_ATTR);

        targetUseridAttr = params.get(TARGET_USERID_ATTR);
        targetGroupingAttr = params.get(TARGET_GROUPING_ATTR);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Map<String, DataStream> execute() {
        String _signalsUseridColumn = signalsUseridAttr;

        // userid -> S
        JavaPairRDD<String, Long> S = ((JavaRDD<Object>) inputStreams.get(RDD_INPUT_SIGNALS).get())
                .mapPartitionsToPair(it -> {
                    List<Tuple2<String, Void>> ret = new ArrayList<>();

                    while (it.hasNext()) {
                        Record row = (Record) it.next();

                        String userid = row.asString(_signalsUseridColumn);

                        ret.add(new Tuple2<>(userid, null));
                    }

                    return ret.iterator();
                })
                .aggregateByKey(0L, (c, v) -> c + 1L, Long::sum);

        String _targetUseridAttr = targetUseridAttr;
        String _targetGroupingAttr = targetGroupingAttr;

        // userid -> groupid, s
        JavaPairRDD<String, Tuple2<String, Long>> s = ((JavaRDD<Object>) inputStreams.get(RDD_INPUT_TARGET).get())
                .mapPartitionsToPair(it -> {
                    List<Tuple2<Tuple2<String, String>, Void>> ret = new ArrayList<>();
                    while (it.hasNext()) {
                        Record row = (Record) it.next();

                        String userid = row.asString(_targetUseridAttr);
                        String groupid = row.asString(_targetGroupingAttr);

                        ret.add(new Tuple2<>(new Tuple2<>(userid, groupid), null));
                    }

                    return ret.iterator();
                })
                .aggregateByKey(0L, (c, v) -> c + 1L, Long::sum)
                .mapToPair(t -> new Tuple2<>(t._1._1, new Tuple2<>(t._1._2, t._2)));

        final List<String> outputColumns = Collections.singletonList(GEN_DWELLTIME);

        JavaPairRDD<String, Columnar> output = s.join(S)
                .mapToPair(t -> new Tuple2<>(t._2._1._1, t._2._1._2.doubleValue() / t._2._2.doubleValue()))
                .aggregateByKey(new Tuple2<>(0L, 0.D),
                        (c, t) -> new Tuple2<>(c._1 + 1L, c._2 + t),
                        (c1, c2) -> new Tuple2<>(c1._1 + c2._1, c1._2 + c2._2)
                )
                .mapToPair(c -> new Tuple2<>(c._1, new Columnar(outputColumns, new Object[]{c._2._2 / c._2._1})));

        return Collections.singletonMap(outputStreams.firstKey(), new DataStream(StreamType.KeyValue, output, Collections.singletonMap(OBJLVL_VALUE, outputColumns)));
    }
}
