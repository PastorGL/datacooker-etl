/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.simplefilters;

import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.metadata.DefinitionMetaBuilder;
import io.github.pastorgl.datacooker.metadata.OperationMeta;
import io.github.pastorgl.datacooker.metadata.Origin;
import io.github.pastorgl.datacooker.metadata.PositionalStreamsMetaBuilder;
import io.github.pastorgl.datacooker.data.Columnar;
import io.github.pastorgl.datacooker.data.DataStream;
import io.github.pastorgl.datacooker.data.StreamType;
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
        return new OperationMeta("percentileFilter", "In a Columnar DataStream, take a column to filter all rows that have" +
                " a Double value in this column that lies outside of the set percentile range",

                new PositionalStreamsMetaBuilder()
                        .input("Columnar DataStream",
                                new StreamType[]{StreamType.Columnar}
                        )
                        .build(),

                new DefinitionMetaBuilder()
                        .def(FILTERING_COLUMN, "Column with Double values to apply the filter")
                        .def(PERCENTILE_BOTTOM, "Bottom of percentile range (inclusive)", Double.class,
                                -1.D, "By default, do not set bottom percentile")
                        .def(PERCENTILE_TOP, "Top of percentile range (inclusive)", Double.class,
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

        if ((topPercentile >= 0) && (bottomPercentile >= 0) && (topPercentile < bottomPercentile)) {
            throw new InvalidConfigurationException("Check if value of '" + PERCENTILE_TOP + "' is greater than value of '" + PERCENTILE_BOTTOM + "' for operation '" + meta.verb + "'");
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
                    boolean match = true;
                    if (_topPercentile >= 0) {
                        match &= t._2._1 <= _top;
                    }
                    if (_bottomPercentile >= 0) {
                        match &= t._2._1 >= _bottom;
                    }

                    return match;
                })
                .map(t -> t._2._2);

        return Collections.singletonMap(outputStreams.firstKey(), new DataStream(StreamType.Columnar, outputRDD, input.accessor.attributes()));
    }
}
