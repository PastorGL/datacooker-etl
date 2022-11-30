/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.simplefilters;

import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.data.Columnar;
import io.github.pastorgl.datacooker.data.DataStream;
import io.github.pastorgl.datacooker.data.StreamType;
import io.github.pastorgl.datacooker.metadata.DefinitionMetaBuilder;
import io.github.pastorgl.datacooker.metadata.OperationMeta;
import io.github.pastorgl.datacooker.metadata.Origin;
import io.github.pastorgl.datacooker.metadata.PositionalStreamsMetaBuilder;
import io.github.pastorgl.datacooker.scripting.Operation;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.Collections;
import java.util.Map;

@SuppressWarnings("unused")
public class PercentileFilterOperation extends Operation {
    public static final String FILTERING_COLUMN = "column";
    public static final String PERCENTILE_TOP = "top";
    public static final String PERCENTILE_BOTTOM = "bottom";

    private String filteringColumn;

    private double topPercentile;
    private double bottomPercentile;

    @Override
    public OperationMeta meta() {
        return new OperationMeta("percentileFilter", "Filter a DataStream by selected Double attribute value" +
                " range, but treated as percentile (0 to 100 percent). Filter can be set below (top is set, bottom isn't)," +
                " above (bottom is set, top isn't), between (bottom is set below top), or outside of (bottom is set above" +
                " top) two selected values",

                new PositionalStreamsMetaBuilder()
                        .input("Columnar DataStream",
                                new StreamType[]{StreamType.Columnar}
                        )
                        .build(),

                new DefinitionMetaBuilder()
                        .def(FILTERING_COLUMN, "Column with Double values to apply the filter")
                        .def(PERCENTILE_BOTTOM, "Bottom of percentile range (inclusive), up to 100", Double.class,
                                -1.D, "By default, do not set bottom percentile")
                        .def(PERCENTILE_TOP, "Top of percentile range (inclusive), up to 100", Double.class,
                                -1.D, "By default, do not set top percentile")
                        .build(),

                new PositionalStreamsMetaBuilder()
                        .output("Filtered CSV RDD",
                                new StreamType[]{StreamType.Columnar}, Origin.FILTERED, null
                        )
                        .build()
        );
    }

    @Override
    public void configure() throws InvalidConfigurationException {
        filteringColumn = params.get(FILTERING_COLUMN);

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
    @SuppressWarnings("unchecked")
    public Map<String, DataStream> execute() {
        DataStream input = inputStreams.getValue(0);
        JavaRDD<Columnar> inputRDD = (JavaRDD<Columnar>) input.get();

        String _filteringColumn = filteringColumn;

        JavaRDD<Tuple2<Double, Columnar>> series = inputRDD
                .map(o -> new Tuple2<>(o.asDouble(_filteringColumn), o));

        JavaPairRDD<Long, Tuple2<Double, Columnar>> percentiles = series
                .sortBy(d -> d._1, true, inputRDD.getNumPartitions())
                .zipWithIndex()
                .mapToPair(Tuple2::swap);

        long count = series.count();

        double top = 0.D, bottom = 0.D;
        if (topPercentile >= 0) {
            top = percentiles.lookup((long) (count * topPercentile / 100.D)).get(0)._1;
        }
        if (bottomPercentile >= 0) {
            bottom = percentiles.lookup((long) (count * bottomPercentile / 100.D)).get(0)._1;
        }

        final double _top = top, _bottom = bottom;
        final double _topPercentile = topPercentile, _bottomPercentile = bottomPercentile;
        JavaRDD<Columnar> outputRDD = percentiles
                .filter(t -> {
                    boolean matches = true;
                    if ((_bottomPercentile >= 0) && (_topPercentile >= 0)) {
                        matches = (_bottomPercentile > _topPercentile) ? ((t._2._1 >= _bottom) || (t._2._1 <= _top)) : ((t._2._1 >= _bottom) && (t._2._1 <= _top));
                    } else {
                        if (_bottomPercentile >= 0) {
                            matches = (t._2._1 >= _bottom);
                        }
                        if (_topPercentile >= 0) {
                            matches = (t._2._1 <= _top);
                        }
                    }

                    return matches;
                })
                .map(t -> t._2._2);

        return Collections.singletonMap(outputStreams.firstKey(), new DataStream(StreamType.Columnar, outputRDD, input.accessor.attributes()));
    }
}
