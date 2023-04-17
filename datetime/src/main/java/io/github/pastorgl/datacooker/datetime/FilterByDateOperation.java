/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.datetime;

import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.data.DataStream;
import io.github.pastorgl.datacooker.data.DateTime;
import io.github.pastorgl.datacooker.data.Record;
import io.github.pastorgl.datacooker.data.StreamType;
import io.github.pastorgl.datacooker.metadata.DefinitionMetaBuilder;
import io.github.pastorgl.datacooker.metadata.OperationMeta;
import io.github.pastorgl.datacooker.metadata.Origin;
import io.github.pastorgl.datacooker.metadata.PositionalStreamsMetaBuilder;
import io.github.pastorgl.datacooker.scripting.Operation;
import org.apache.spark.api.java.JavaRDD;

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
        return new OperationMeta("filterByDate", "Filter a DataStream by selected 'timestamp' attribute" +
                " value. Filter can be set before (start is set, end isn't), after (end is set, start isn't)," +
                " between (start is set before end), or outside of (start is set after end) two selected dates",

                new PositionalStreamsMetaBuilder()
                        .input("Source DataSteam with a 'timestamp' attribute",
                                new StreamType[]{StreamType.Columnar, StreamType.Structured, StreamType.Point, StreamType.Polygon, StreamType.Track}
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
                        .output("DataStream, filtered by date range",
                                new StreamType[]{StreamType.Columnar, StreamType.Structured, StreamType.Point, StreamType.Polygon, StreamType.Track}, Origin.FILTERED, null
                        )
                        .build()
        );
    }

    @Override
    public void configure() throws InvalidConfigurationException {
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

    @SuppressWarnings("rawtypes")
    @Override
    public Map<String, DataStream> execute() {
        final String _col = dateAttr;
        final Date _start = start;
        final Date _end = end;

        DataStream input = inputStreams.getValue(0);
        JavaRDD<Object> output = ((JavaRDD<Object>) input.get())
                .mapPartitions(it -> {
                    List<Object> ret = new ArrayList<>();

                    Calendar cc = Calendar.getInstance();
                    Date ccTime;
                    while (it.hasNext()) {
                        Record v = (Record) it.next();

                        cc.setTime(DateTime.parseTimestamp(v.asIs(_col)));
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

        return Collections.singletonMap(outputStreams.firstKey(), new DataStream(input.streamType, output, input.accessor.attributes()));
    }
}
