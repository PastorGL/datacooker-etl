/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.datetime;

import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.metadata.DefinitionMetaBuilder;
import io.github.pastorgl.datacooker.metadata.OperationMeta;
import io.github.pastorgl.datacooker.metadata.Origin;
import io.github.pastorgl.datacooker.metadata.PositionalStreamsMetaBuilder;
import io.github.pastorgl.datacooker.data.Columnar;
import io.github.pastorgl.datacooker.data.DataStream;
import io.github.pastorgl.datacooker.data.DateTime;
import io.github.pastorgl.datacooker.data.StreamType;
import io.github.pastorgl.datacooker.scripting.Operation;
import org.apache.spark.api.java.JavaRDD;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.*;

import static io.github.pastorgl.datacooker.config.Constants.OBJLVL_VALUE;

@SuppressWarnings("unused")
public class TimezoneOperation extends Operation {
    public static final String SOURCE_TS_COLUMN = "source_ts_column";

    public static final String SOURCE_TS_FORMAT = "source_ts_format";
    public static final String SOURCE_TZ_DEFAULT = "source_tz_default";
    public static final String SOURCE_TZ_COLUMN = "source_tz_column";

    public static final String DEST_TS_FORMAT = "dest_ts_format";
    public static final String DEST_TZ_DEFAULT = "dest_tz_default";
    public static final String DEST_TZ_COLUMN = "dest_tz_column";

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

    private String timestampColumn;
    private String timezoneColumn;
    private String outputTimezoneColumn;

    private String timestampFormat;
    private String outputTimestampFormat;
    private TimeZone sourceTimezoneDefault;
    private TimeZone destinationTimezoneDefault;

    @Override
    public OperationMeta meta() {
        return new OperationMeta("timezone", "Take a Columnar DataStream with a timestamp column (Epoch seconds or" +
                " milliseconds, ISO of custom format) and explode its components into individual attributes." +
                " Perform timezone conversion, using source and destination timezones from the parameters or" +
                " another source attributes",

                new PositionalStreamsMetaBuilder()
                        .input("Columnar RDD with timestamp and optional timezone attributes",
                                new StreamType[]{StreamType.Columnar}
                        )
                        .build(),

                new DefinitionMetaBuilder()
                        .def(SOURCE_TS_COLUMN, "Source column with a timestamp")
                        .def(SOURCE_TS_FORMAT, "If set, use this format to parse source timestamp", null, "By default, use ISO formatting for the full source date")
                        .def(SOURCE_TZ_COLUMN, "If set, use source timezone from this column instead of the default", null, "By default, do not read source time zone from input column")
                        .def(SOURCE_TZ_DEFAULT, "Source timezone default", "GMT", "By default, source time zone is GMT")
                        .def(DEST_TS_FORMAT, "If set, use this format to output full date", null, "By default, use ISO formatting for the full destination date")
                        .def(DEST_TZ_COLUMN, "If set, use destination timezone from this column instead of the default", null, "By default, do not read destination time zone from input column")
                        .def(DEST_TZ_DEFAULT, "Destination timezone default", "GMT", "By default, destination time zone is GMT")
                        .build(),

                new PositionalStreamsMetaBuilder()
                        .output("Columnar RDD with exploded timestamp component attributes",
                                new StreamType[]{StreamType.Columnar}, Origin.AUGMENTED, null
                        )
                        .generated(GEN_INPUT_DATE, "Generated full input date column")
                        .generated(GEN_INPUT_DOW_INT, "Generated input day of week column")
                        .generated(GEN_INPUT_DAY_INT, "Generated input date of month column")
                        .generated(GEN_INPUT_MONTH_INT, "Generated input month column")
                        .generated(GEN_INPUT_YEAR_INT, "Generated input year column")
                        .generated(GEN_INPUT_HOUR_INT, "Generated input hour column")
                        .generated(GEN_INPUT_MINUTE_INT, "Generated input minute column")
                        .generated(GEN_OUTPUT_DATE, "Generated full output date column")
                        .generated(GEN_OUTPUT_DOW_INT, "Generated output day of week column")
                        .generated(GEN_OUTPUT_DAY_INT, "Generated output date of month column")
                        .generated(GEN_OUTPUT_MONTH_INT, "Generated output month column")
                        .generated(GEN_OUTPUT_YEAR_INT, "Generated output year column")
                        .generated(GEN_OUTPUT_HOUR_INT, "Generated output hour column")
                        .generated(GEN_OUTPUT_MINUTE_INT, "Generated output minute column")
                        .generated(GEN_EPOCH_TIME, "Generated Epoch time of the timestamp")
                        .build()
        );
    }

    @Override
    public void configure() throws InvalidConfigurationException {
        timestampColumn = params.get(SOURCE_TS_COLUMN);
        timezoneColumn = params.get(SOURCE_TZ_COLUMN);
        if (timezoneColumn == null) {
            String timezoneDefault = params.get(SOURCE_TZ_DEFAULT);
            sourceTimezoneDefault = TimeZone.getTimeZone(timezoneDefault);
        }
        timestampFormat = params.get(SOURCE_TS_FORMAT);

        outputTimezoneColumn = params.get(DEST_TZ_COLUMN);
        if (outputTimezoneColumn == null) {
            String outputTimezoneDefault = params.get(DEST_TZ_DEFAULT);
            destinationTimezoneDefault = TimeZone.getTimeZone(outputTimezoneDefault);
        }
        outputTimestampFormat = params.get(DEST_TS_FORMAT);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Map<String, DataStream> execute() {
        final String _sourceTimestampColumn = timestampColumn;
        final String _sourceTimezoneColumn = timezoneColumn;
        final TimeZone _sourceTimezoneDefault = sourceTimezoneDefault;
        final String _sourceTimestampFormat = timestampFormat;

        final String _destinationTimezoneColumn = outputTimezoneColumn;
        final TimeZone _destinationTimezoneDefault = destinationTimezoneDefault;
        final String _destinationTimestampFormat = outputTimestampFormat;

        final List<String> _columns = new ArrayList<>(inputStreams.getValue(0).accessor.attributes(OBJLVL_VALUE));
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

        JavaRDD<Columnar> output = ((JavaRDD<Columnar>) inputStreams.getValue(0).get())
                .mapPartitions(it -> {
                    ZoneId GMT = TimeZone.getTimeZone("GMT").toZoneId();

                    DateTimeFormatter dtfInput = (_sourceTimestampFormat != null)
                            ? new DateTimeFormatterBuilder().appendPattern(_sourceTimestampFormat).toFormatter()
                            : null;
                    DateTimeFormatter dtfOutput = (_destinationTimestampFormat != null)
                            ? new DateTimeFormatterBuilder().appendPattern(_destinationTimestampFormat).toFormatter()
                            : null;

                    List<Columnar> result = new ArrayList<>();
                    while (it.hasNext()) {
                        Columnar line = it.next();

                        long timestamp;
                        Object tsObject = line.asIs(_sourceTimestampColumn);

                        ZoneId inputTimezone = (_sourceTimezoneColumn == null)
                                ? _sourceTimezoneDefault.toZoneId()
                                : TimeZone.getTimeZone(line.asString(_sourceTimezoneColumn)).toZoneId();

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

                        ZoneId outputTimezone = (_destinationTimezoneColumn == null)
                                ? _destinationTimezoneDefault.toZoneId()
                                : TimeZone.getTimeZone(line.asString(_destinationTimezoneColumn)).toZoneId();

                        ZonedDateTime outputDate = inputDate.toInstant().atZone(outputTimezone);

                        Columnar rec = new Columnar(_columns);
                        rec.put(line.asIs());
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

                        result.add(rec);
                    }

                    return result.iterator();
                });

        return Collections.singletonMap(outputStreams.firstKey(), new DataStream(StreamType.Columnar, output, Collections.singletonMap(OBJLVL_VALUE, _columns)));
    }
}
