/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.simplefilters;

import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.data.DataStream;
import io.github.pastorgl.datacooker.data.DataStreamBuilder;
import io.github.pastorgl.datacooker.data.Record;
import io.github.pastorgl.datacooker.data.StreamType;
import io.github.pastorgl.datacooker.metadata.DefinitionMetaBuilder;
import io.github.pastorgl.datacooker.metadata.OperationMeta;
import io.github.pastorgl.datacooker.metadata.PositionalStreamsMetaBuilder;
import io.github.pastorgl.datacooker.metadata.StreamOrigin;
import io.github.pastorgl.datacooker.scripting.Operation;
import org.apache.commons.collections4.map.SingletonMap;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.github.pastorgl.datacooker.Constants.OBJLVL_VALUE;

@SuppressWarnings("unused")
public class PercentileFilterOperation extends Operation {
    public static final String FILTERING_ATTR = "attr";
    public static final String PERCENTILE_TOP = "top";
    public static final String PERCENTILE_BOTTOM = "bottom";

    private static final String GEN_PERCENTILE = "_percentile";

    private String filteringColumn;

    private double topPercentile;
    private double bottomPercentile;

    @Override
    public OperationMeta meta() {
        return new OperationMeta("percentileFilter", "Filter DataStreams by selected Double attribute value" +
                " range, but treated as percentile (0 to 100 percent). Filter can be set below (top is set, bottom isn't)," +
                " above (bottom is set, top isn't), between (bottom is set below top), or outside of (bottom is set above" +
                " top) two selected values. Names of referenced attributes have to be same in each INPUT DataStream",

                new PositionalStreamsMetaBuilder()
                        .input("INPUT DataStream", StreamType.ATTRIBUTED)
                        .build(),

                new DefinitionMetaBuilder()
                        .def(FILTERING_ATTR, "Column with Double values to apply the filter")
                        .def(PERCENTILE_BOTTOM, "Bottom of percentile range (inclusive), up to 100", Double.class,
                                -1.D, "By default, do not set bottom percentile")
                        .def(PERCENTILE_TOP, "Top of percentile range (inclusive), up to 100", Double.class,
                                -1.D, "By default, do not set top percentile")
                        .build(),

                new PositionalStreamsMetaBuilder()
                        .output("Filtered DataStream with percentile",
                                StreamType.ATTRIBUTED, StreamOrigin.AUGMENTED, null
                        )
                        .generated(GEN_PERCENTILE, "Percentile value")
                        .build()
        );
    }

    @Override
    public void configure() throws InvalidConfigurationException {
        filteringColumn = params.get(FILTERING_ATTR);

        topPercentile = params.get(PERCENTILE_TOP);
        if (topPercentile > 100) {
            topPercentile = 100;
        }

        bottomPercentile = params.get(PERCENTILE_BOTTOM);
        if (bottomPercentile > 100) {
            bottomPercentile = 100;
        }

        if ((topPercentile < 0) && (bottomPercentile < 0)) {
            throw new InvalidConfigurationException("Check if '" + PERCENTILE_TOP + "' and/or '" + PERCENTILE_BOTTOM + "' for operation '" + meta.verb + "' are set");
        }
    }

    @Override
    public Map<String, DataStream> execute() {
        if (inputStreams.size() != outputStreams.size()) {
            throw new InvalidConfigurationException("Operation '" + meta.verb + "' requires same amount of INPUT and OUTPUT streams");
        }

        String _filteringColumn = filteringColumn;

        Map<String, DataStream> output = new HashMap<>();
        for (int i = 0, len = inputStreams.size(); i < len; i++) {
            DataStream input = inputStreams.getValue(i);

            JavaPairRDD<Long, Tuple3<Object, Record<?>, Double>> percentiles = input.rdd
                    .mapToPair(o -> new Tuple2<>(o._2.asDouble(_filteringColumn), o))
                    .sortByKey(true, input.rdd.getNumPartitions())
                    .zipWithIndex()
                    .mapToPair(t -> new Tuple2<>(t._2, new Tuple3<>(t._1._2._1, t._1._2._2, t._1._1)));

            long count = input.rdd.count();

            double top = 0.D, bottom = 0.D;
            if (topPercentile >= 0) {
                top = percentiles.lookup((long) (count * topPercentile / 100.D)).get(0)._3();
            }
            if (bottomPercentile >= 0) {
                bottom = percentiles.lookup((long) (count * bottomPercentile / 100.D)).get(0)._3();
            }

            final double _top = top, _bottom = bottom;
            final double _topPercentile = topPercentile, _bottomPercentile = bottomPercentile;
            JavaPairRDD<Object, Record<?>> out = percentiles
                    .filter(t -> {
                        boolean matches = true;
                        if ((_bottomPercentile >= 0) && (_topPercentile >= 0)) {
                            matches = (_bottomPercentile > _topPercentile)
                                    ? ((t._2._3() >= _bottom) || (t._2._3() <= _top))
                                    : ((t._2._3() >= _bottom) && (t._2._3() <= _top));
                        } else {
                            if (_bottomPercentile >= 0) {
                                matches = (t._2._3() >= _bottom);
                            }
                            if (_topPercentile >= 0) {
                                matches = (t._2._3() <= _top);
                            }
                        }

                        return matches;
                    })
                    .mapToPair(t -> {
                        Record<?> rec = (Record<?>) t._2._2().clone();
                        rec.put(GEN_PERCENTILE, t._2._3());

                        return new Tuple2<>(t._2._1(), rec);
                    });

            List<String> outColumns = new ArrayList<>(input.accessor.attributes().get(OBJLVL_VALUE));
            outColumns.add(GEN_PERCENTILE);

            output.put(outputStreams.get(i), new DataStreamBuilder(outputStreams.get(i), input.streamType, new SingletonMap<>(OBJLVL_VALUE, outColumns))
                    .augmented(meta.verb, input)
                    .build(out)
            );
        }

        return output;
    }
}
