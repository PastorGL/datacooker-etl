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

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.*;

import static io.github.pastorgl.datacooker.Constants.OBJLVL_VALUE;

@SuppressWarnings("unused")
public class TimezoneOperation extends Operation {
    public static final String SOURCE_TS_ATTR = "source_ts_attr";

    public static final String SOURCE_TS_FORMAT = "source_ts_format";
    public static final String SOURCE_TZ_DEFAULT = "source_tz_default";
    public static final String SOURCE_TZ_ATTR = "source_tz_attr";

    public static final String DEST_TS_FORMAT = "dest_ts_format";
    public static final String DEST_TZ_DEFAULT = "dest_tz_default";
    public static final String DEST_TZ_ATTR = "dest_tz_attr";

    public static final String GEN_INPUT_DATE = "_input_date";
    public static final String GEN_INPUT_DOW_INT = "_input_dow_int";
    public static final String GEN_INPUT_DAY_INT = "_input_day_int";
    public static final String GEN_INPUT_MONTH_INT = "_input_month_int";
    public static final String GEN_INPUT_YEAR_INT = "_input_year_int";
    public static final String GEN_INPUT_HOUR_INT = "_input_hour_int";
    public static final String GEN_INPUT_MINUTE_INT = "_input_minute_int";
    public static final String GEN_OUTPUT_DATE = "_output_date";
    public static final String GEN_OUTPUT_DOW_INT = "_output_dow_int";
    public static final String GEN_OUTPUT_DAY_INT = "_output_day_int";
    public static final String GEN_OUTPUT_MONTH_INT = "_output_month_int";
    public static final String GEN_OUTPUT_YEAR_INT = "_output_year_int";
    public static final String GEN_OUTPUT_HOUR_INT = "_output_hour_int";
    public static final String GEN_OUTPUT_MINUTE_INT = "_output_minute_int";
    public static final String GEN_EPOCH_TIME = "_epoch_time";

    private String timestampAttr;
    private String timezoneAttr;
    private String outputTimezoneAttr;

    private String timestampFormat;
    private String outputTimestampFormat;
    private TimeZone sourceTimezoneDefault;
    private TimeZone destinationTimezoneDefault;

    @Override
    public OperationMeta meta() {
        return new OperationMeta("timezone", "Take DataStreams with a 'timestamp' attribute (Epoch seconds or" +
                " milliseconds, ISO of custom format) and explode its value into individual attributes with date and time" +
                " components. Optionally perform timezone conversion, using source and destination timezones from the" +
                " parameters or another source attributes. Attribute names are same for all input streams",

                new PositionalStreamsMetaBuilder()
                        .input("DataStreams with timestamp and optional timezone attributes",
                                StreamType.ATTRIBUTED
                        )
                        .build(),

                new DefinitionMetaBuilder()
                        .def(SOURCE_TS_ATTR, "Source attribute with a timestamp")
                        .def(SOURCE_TS_FORMAT, "If set, use this format to parse source timestamp", null,
                                "By default, use ISO formatting for the full source date")
                        .def(SOURCE_TZ_ATTR, "If set, use source timezone from this attribute instead of the default", null,
                                "By default, do not read source time zone from input attribute")
                        .def(SOURCE_TZ_DEFAULT, "Source timezone default", "GMT",
                                "By default, source time zone is GMT")
                        .def(DEST_TS_FORMAT, "If set, use this format to output full date", null,
                                "By default, use ISO formatting for the full destination date")
                        .def(DEST_TZ_ATTR, "If set, use destination timezone from this attribute instead of the default", null,
                                "By default, do not read destination time zone from input attribute")
                        .def(DEST_TZ_DEFAULT, "Destination timezone default", "GMT",
                                "By default, destination time zone is GMT")
                        .build(),

                new PositionalStreamsMetaBuilder()
                        .output("OUTPUT DataStreams with exploded timestamp component attributes",
                                StreamType.ATTRIBUTED, StreamOrigin.AUGMENTED, null
                        )
                        .generated(GEN_INPUT_DATE, "Input date")
                        .generated(GEN_INPUT_DOW_INT, "Input day of week")
                        .generated(GEN_INPUT_DAY_INT, "Input date of month")
                        .generated(GEN_INPUT_MONTH_INT, "Input month")
                        .generated(GEN_INPUT_YEAR_INT, "Input year")
                        .generated(GEN_INPUT_HOUR_INT, "Input hour")
                        .generated(GEN_INPUT_MINUTE_INT, "Input minute")
                        .generated(GEN_OUTPUT_DATE, "Converted date")
                        .generated(GEN_OUTPUT_DOW_INT, "Converted day of week")
                        .generated(GEN_OUTPUT_DAY_INT, "Converted date of month")
                        .generated(GEN_OUTPUT_MONTH_INT, "Converted month")
                        .generated(GEN_OUTPUT_YEAR_INT, "Converted year")
                        .generated(GEN_OUTPUT_HOUR_INT, "Converted hour")
                        .generated(GEN_OUTPUT_MINUTE_INT, "Converted minute")
                        .generated(GEN_EPOCH_TIME, "Epoch seconds of the timestamp")
                        .build()
        );
    }

    @Override
    public void configure(Configuration params) throws InvalidConfigurationException {
        timestampAttr = params.get(SOURCE_TS_ATTR);
        timezoneAttr = params.get(SOURCE_TZ_ATTR);
        if (timezoneAttr == null) {
            String timezoneDefault = params.get(SOURCE_TZ_DEFAULT);
            sourceTimezoneDefault = TimeZone.getTimeZone(timezoneDefault);
        }
        timestampFormat = params.get(SOURCE_TS_FORMAT);

        outputTimezoneAttr = params.get(DEST_TZ_ATTR);
        if (outputTimezoneAttr == null) {
            String outputTimezoneDefault = params.get(DEST_TZ_DEFAULT);
            destinationTimezoneDefault = TimeZone.getTimeZone(outputTimezoneDefault);
        }
        outputTimestampFormat = params.get(DEST_TS_FORMAT);
    }

    @Override
    public ListOrderedMap<String, DataStream> execute() {
        if (inputStreams.size() != outputStreams.size()) {
            throw new InvalidConfigurationException("Operation '" + meta.verb + "' requires same amount of INPUT and OUTPUT streams");
        }

        final String _sourceTimestampAttr = timestampAttr;
        final String _sourceTimezoneAttr = timezoneAttr;
        final TimeZone _sourceTimezoneDefault = sourceTimezoneDefault;
        final String _sourceTimestampFormat = timestampFormat;

        final String _destinationTimezoneAttr = outputTimezoneAttr;
        final TimeZone _destinationTimezoneDefault = destinationTimezoneDefault;
        final String _destinationTimestampFormat = outputTimestampFormat;

        ListOrderedMap<String, DataStream> outputs = new ListOrderedMap<>();
        for (int i = 0, len = inputStreams.size(); i < len; i++) {
            DataStream input = inputStreams.getValue(i);

            final List<String> _columns = new ArrayList<>(input.accessor.attributes(OBJLVL_VALUE));
            _columns.add(GEN_INPUT_DATE);
            _columns.add(GEN_INPUT_DOW_INT);
            _columns.add(GEN_INPUT_DAY_INT);
            _columns.add(GEN_INPUT_MONTH_INT);
            _columns.add(GEN_INPUT_YEAR_INT);
            _columns.add(GEN_INPUT_HOUR_INT);
            _columns.add(GEN_INPUT_MINUTE_INT);
            _columns.add(GEN_OUTPUT_DATE);
            _columns.add(GEN_OUTPUT_DOW_INT);
            _columns.add(GEN_OUTPUT_DAY_INT);
            _columns.add(GEN_OUTPUT_MONTH_INT);
            _columns.add(GEN_OUTPUT_YEAR_INT);
            _columns.add(GEN_OUTPUT_HOUR_INT);
            _columns.add(GEN_OUTPUT_MINUTE_INT);
            _columns.add(GEN_EPOCH_TIME);

            JavaPairRDD<Object, Record<?>> out = input.rdd.mapPartitionsToPair(it -> {
                ZoneId GMT = TimeZone.getTimeZone("GMT").toZoneId();

                DateTimeFormatter dtfInput = (_sourceTimestampFormat != null)
                        ? new DateTimeFormatterBuilder().appendPattern(_sourceTimestampFormat).toFormatter()
                        : null;
                DateTimeFormatter dtfOutput = (_destinationTimestampFormat != null)
                        ? new DateTimeFormatterBuilder().appendPattern(_destinationTimestampFormat).toFormatter()
                        : null;

                List<Tuple2<Object, Record<?>>> result = new ArrayList<>();
                while (it.hasNext()) {
                    Tuple2<Object, Record<?>> next = it.next();

                    long timestamp;
                    Object tsObject = next._2.asIs(_sourceTimestampAttr);

                    ZoneId inputTimezone = (_sourceTimezoneAttr == null)
                            ? _sourceTimezoneDefault.toZoneId()
                            : TimeZone.getTimeZone(next._2.asString(_sourceTimezoneAttr)).toZoneId();

                    if (dtfInput != null) {
                        timestamp = Date.from(Instant.from(dtfInput.withZone(inputTimezone).parse(String.valueOf(tsObject)))).getTime();
                    } else {
                        timestamp = DateTime.parseTimestamp(tsObject).getTime();
                    }

                    LocalDateTime localGMTDate = LocalDateTime.ofInstant(new Date(timestamp).toInstant(), GMT);

                    ZonedDateTime inputDate = ZonedDateTime.of(
                            localGMTDate.getYear(),
                            localGMTDate.getMonth().getValue(),
                            localGMTDate.getDayOfMonth(),
                            localGMTDate.getHour(),
                            localGMTDate.getMinute(),
                            localGMTDate.getSecond(),
                            localGMTDate.getNano(),
                            inputTimezone
                    );

                    ZoneId outputTimezone = (_destinationTimezoneAttr == null)
                            ? _destinationTimezoneDefault.toZoneId()
                            : TimeZone.getTimeZone(next._2.asString(_destinationTimezoneAttr)).toZoneId();

                    ZonedDateTime outputDate = inputDate.toInstant().atZone(outputTimezone);

                    Record<?> rec = (Record<?>) next._2.clone();
                    rec.put(GEN_INPUT_DATE, (dtfOutput != null) ? dtfOutput.withZone(inputTimezone).format(inputDate) : inputDate.toString());
                    rec.put(GEN_INPUT_DOW_INT, inputDate.getDayOfWeek().getValue());
                    rec.put(GEN_INPUT_DAY_INT, inputDate.getDayOfMonth());
                    rec.put(GEN_INPUT_MONTH_INT, inputDate.getMonthValue());
                    rec.put(GEN_INPUT_YEAR_INT, inputDate.getYear());
                    rec.put(GEN_INPUT_HOUR_INT, inputDate.getHour());
                    rec.put(GEN_INPUT_MINUTE_INT, inputDate.getMinute());
                    rec.put(GEN_OUTPUT_DATE, (dtfOutput != null) ? dtfOutput.withZone(outputTimezone).format(outputDate) : outputDate.toString());
                    rec.put(GEN_OUTPUT_DOW_INT, outputDate.getDayOfWeek().getValue());
                    rec.put(GEN_OUTPUT_DAY_INT, outputDate.getDayOfMonth());
                    rec.put(GEN_OUTPUT_MONTH_INT, outputDate.getMonthValue());
                    rec.put(GEN_OUTPUT_YEAR_INT, outputDate.getYear());
                    rec.put(GEN_OUTPUT_HOUR_INT, outputDate.getHour());
                    rec.put(GEN_OUTPUT_MINUTE_INT, outputDate.getMinute());
                    rec.put(GEN_EPOCH_TIME, localGMTDate.toEpochSecond(ZoneOffset.UTC));

                    result.add(new Tuple2<>(next._1, rec));
                }

                return result.iterator();
            });

            outputs.put(outputStreams.get(i), new DataStreamBuilder(outputStreams.get(i), input.streamType, Collections.singletonMap(OBJLVL_VALUE, _columns))
                    .augmented(meta.verb, input)
                    .build(out)
            );
        }

        return outputs;
    }
}
