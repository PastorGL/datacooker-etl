/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.datetime;

import io.github.pastorgl.datacooker.config.Configuration;
import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.data.*;
import io.github.pastorgl.datacooker.metadata.DefinitionMetaBuilder;
import io.github.pastorgl.datacooker.metadata.OperationMeta;
import io.github.pastorgl.datacooker.metadata.PositionalStreamsMetaBuilder;
import io.github.pastorgl.datacooker.data.StreamOrigin;
import io.github.pastorgl.datacooker.scripting.Operation;
import org.apache.commons.collections4.map.ListOrderedMap;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.util.*;

@SuppressWarnings("unused")
public class FilterByDateOperation extends Operation {
    public static final String TS_ATTR = "ts_attr";
    public static final String START = "start";
    public static final String END = "end";

    private String dateAttr;

    private Date start = null;
    private Date end = null;

    @Override
    public OperationMeta meta() {
        return new OperationMeta("filterByDate", "Filter DataStreams by 'timestamp' attribute (same for all)." +
                " Filter can be set before (start is set, end isn't), after (end is set, start isn't)," +
                " between (start is set before end), or outside of (start is set after end) two selected dates",

                new PositionalStreamsMetaBuilder()
                        .input("DataSteams with a 'timestamp' attribute",
                                StreamType.ATTRIBUTED
                        )
                        .build(),

                new DefinitionMetaBuilder()
                        .def(TS_ATTR, "Attribute with 'timestamp' in Epoch seconds, milliseconds, or as ISO string")
                        .def(START, "Start of the date range filter (same format)",
                                null, "By default do not filter by range start")
                        .def(END, "End of the date range filter (same format)",
                                null, "By default do not filter by range end")
                        .build(),

                new PositionalStreamsMetaBuilder()
                        .output("DataStreams, filtered by date range",
                                StreamType.ATTRIBUTED, StreamOrigin.FILTERED, null
                        )
                        .build()
        );
    }

    @Override
    public void configure(Configuration params) throws InvalidConfigurationException {
        dateAttr = params.get(TS_ATTR);

        String prop = params.get(START);
        if ((prop != null) && !prop.isEmpty()) {
            start = DateTime.parseTimestamp(prop);
        }
        prop = params.get(END);
        if ((prop != null) && !prop.isEmpty()) {
            end = DateTime.parseTimestamp(prop);
        }

        if ((start == null) && (end == null)) {
            throw new InvalidConfigurationException("Filter by date was not configured for the Operation '" + meta.verb + "'");
        }
    }

    @Override
    public ListOrderedMap<String, DataStream> execute() {
        if (inputStreams.size() != outputStreams.size()) {
            throw new InvalidConfigurationException("Operation '" + meta.verb + "' requires same amount of INPUT and OUTPUT streams");
        }

        final String _col = dateAttr;
        final Date _start = start;
        final Date _end = end;

        ListOrderedMap<String, DataStream> outputs = new ListOrderedMap<>();
        for (int i = 0, len = inputStreams.size(); i < len; i++) {
            DataStream input = inputStreams.getValue(i);

            JavaPairRDD<Object, Record<?>> out = input.rdd.mapPartitionsToPair(it -> {
                List<Tuple2<Object, Record<?>>> ret = new ArrayList<>();

                Calendar cc = Calendar.getInstance();
                Date ccTime;
                while (it.hasNext()) {
                    Tuple2<Object, Record<?>> v = it.next();

                    cc.setTime(DateTime.parseTimestamp(v._2.asIs(_col)));
                    ccTime = cc.getTime();

                    boolean matches = true;
                    if ((_start != null) && (_end != null)) {
                        matches = _start.after(_end) ? (ccTime.after(_start) || ccTime.before(_end)) : (ccTime.after(_start) && ccTime.before(_end));
                    } else {
                        if (_start != null) {
                            matches = ccTime.after(_start);
                        }
                        if (_end != null) {
                            matches = ccTime.before(_end);
                        }
                    }

                    if (matches) {
                        ret.add(v);
                    }
                }

                return ret.iterator();
            });

            outputs.put(outputStreams.get(i), new DataStreamBuilder(outputStreams.get(i), input.streamType, input.accessor.attributes())
                    .filtered(meta.verb, input)
                    .build(out)
            );
        }

        return outputs;
    }
}
