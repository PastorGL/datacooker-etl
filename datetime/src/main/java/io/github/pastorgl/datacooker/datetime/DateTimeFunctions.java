/**
 * Copyright (C) 2024 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.datetime;

import io.github.pastorgl.datacooker.data.DateTime;
import io.github.pastorgl.datacooker.scripting.Evaluator;
import io.github.pastorgl.datacooker.scripting.Function.Binary;
import io.github.pastorgl.datacooker.scripting.Function.Ternary;
import io.github.pastorgl.datacooker.scripting.Function.Unary;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Deque;
import java.util.TimeZone;

@SuppressWarnings("unused")
public class DateTimeFunctions {
    public static class DateBefore extends Binary<Boolean, Object, Object> {
        @Override
        public Boolean call(Deque<Object> args) {
            String end = Evaluator.popString(args);
            String testing = Evaluator.popString(args);

            return DateTime.parseTimestamp(testing).before(DateTime.parseTimestamp(end));
        }

        @Override
        public String name() {
            return "DT_BEFORE";
        }

        @Override
        public String descr() {
            return "Returns TRUE if DateTime from 1st argument is before 2nd argument, FALSE otherwise";
        }
    }

    public static class DateAfter extends Binary<Boolean, Object, Object> {
        @Override
        public Boolean call(Deque<Object> args) {
            String begin = Evaluator.popString(args);
            String testing = Evaluator.popString(args);

            return DateTime.parseTimestamp(testing).after(DateTime.parseTimestamp(begin));
        }

        @Override
        public String name() {
            return "DT_AFTER";
        }

        @Override
        public String descr() {
            return "Returns TRUE if DateTime from 1st argument is after 2nd argument, FALSE otherwise";
        }
    }

    public static class DateDOMTZ extends Binary<Integer, String, Object> {
        @Override
        public Integer call(Deque<Object> args) {
            ZoneId tz = TimeZone.getTimeZone(Evaluator.popString(args)).toZoneId();
            return DateTime.parseTimestamp(Evaluator.popString(args)).toInstant().atZone(tz).getDayOfMonth();
        }

        @Override
        public String name() {
            return "DT_DAY";
        }

        @Override
        public String descr() {
            return "Returns Day of Month component of DateTime given as 2nd argument in the TimeZone set as 1st";
        }
    }

    public static class DateDOWTZ extends Binary<Integer, String, Object> {
        @Override
        public Integer call(Deque<Object> args) {
            ZoneId tz = TimeZone.getTimeZone(Evaluator.popString(args)).toZoneId();
            return DateTime.parseTimestamp(Evaluator.popString(args)).toInstant().atZone(tz).getDayOfWeek().getValue();
        }

        @Override
        public String name() {
            return "DT_DOW";
        }

        @Override
        public String descr() {
            return "Returns Day of Week component of DateTime given as 2nd argument in the TimeZone set as 1st";
        }
    }

    public static class DateMONTHTZ extends Binary<Integer, String, Object> {
        @Override
        public Integer call(Deque<Object> args) {
            ZoneId tz = TimeZone.getTimeZone(Evaluator.popString(args)).toZoneId();
            return DateTime.parseTimestamp(Evaluator.popString(args)).toInstant().atZone(tz).getMonth().getValue();
        }

        @Override
        public String name() {
            return "DT_MONTH";
        }

        @Override
        public String descr() {
            return "Returns Month component of DateTime given as 2nd argument in the TimeZone set as 1st";
        }
    }

    public static class DateYEARTZ extends Binary<Integer, String, Object> {
        @Override
        public Integer call(Deque<Object> args) {
            ZoneId tz = TimeZone.getTimeZone(Evaluator.popString(args)).toZoneId();
            return DateTime.parseTimestamp(Evaluator.popString(args)).toInstant().atZone(tz).getYear();
        }

        @Override
        public String name() {
            return "DT_YEAR";
        }

        @Override
        public String descr() {
            return "Returns Year component of DateTime given as 2nd argument in the TimeZone set as 1st";
        }
    }

    public static class DateHOURTZ extends Binary<Integer, String, Object> {
        @Override
        public Integer call(Deque<Object> args) {
            ZoneId tz = TimeZone.getTimeZone(Evaluator.popString(args)).toZoneId();
            return DateTime.parseTimestamp(Evaluator.popString(args)).toInstant().atZone(tz).getHour();
        }

        @Override
        public String name() {
            return "DT_HOUR";
        }

        @Override
        public String descr() {
            return "Returns Hour component of DateTime given as 2nd argument in the TimeZone set as 1st";
        }
    }

    public static class DateMINUTETZ extends Binary<Integer, String, Object> {
        @Override
        public Integer call(Deque<Object> args) {
            ZoneId tz = TimeZone.getTimeZone(Evaluator.popString(args)).toZoneId();
            return DateTime.parseTimestamp(Evaluator.popString(args)).toInstant().atZone(tz).getMinute();
        }

        @Override
        public String name() {
            return "DT_MINUTE";
        }

        @Override
        public String descr() {
            return "Returns Minutes of Hour component of DateTime given as 2nd argument in the TimeZone set as 1st";
        }
    }

    public static class DateSECONDTZ extends Binary<Integer, String, Object> {
        @Override
        public Integer call(Deque<Object> args) {
            ZoneId tz = TimeZone.getTimeZone(Evaluator.popString(args)).toZoneId();
            return DateTime.parseTimestamp(Evaluator.popString(args)).toInstant().atZone(tz).getSecond();
        }

        @Override
        public String name() {
            return "DT_SECOND";
        }

        @Override
        public String descr() {
            return "Returns Seconds of Minute component of DateTime given as 2nd argument in the TimeZone set as 1st";
        }
    }

    public static class DateTZ extends Binary<Object, String, Object> {
        @Override
        public Object call(Deque<Object> args) {
            ZoneId tz = TimeZone.getTimeZone(Evaluator.popString(args)).toZoneId();
            return DateTime.parseTimestamp(Evaluator.popString(args)).toInstant().atZone(tz).toString();
        }

        @Override
        public String name() {
            return "DT_TZ";
        }

        @Override
        public String descr() {
            return "Returns DateTime given as 2nd argument in the TimeZone set as 1st";
        }
    }

    public static class DateEPOCH extends Unary<Long, Object> {
        @Override
        public Long call(Deque<Object> args) {
            return DateTime.parseTimestamp(Evaluator.popString(args)).getTime() / 1000L;
        }

        @Override
        public String name() {
            return "DT_EPOCH";
        }

        @Override
        public String descr() {
            return "Returns Seconds of Epoch component of given DateTime";
        }
    }

    public static class DatePARSE extends Ternary<Object, String, String, String> {
        @Override
        public Object call(Deque<Object> args) {
            ZoneId tz = TimeZone.getTimeZone(Evaluator.popString(args)).toZoneId();
            DateTimeFormatter dtf = new DateTimeFormatterBuilder().appendPattern(Evaluator.popString(args)).toFormatter().withZone(tz);
            return dtf.parse(Evaluator.popString(args)).getLong(ChronoField.INSTANT_SECONDS);
        }

        @Override
        public String name() {
            return "DT_PARSE";
        }

        @Override
        public String descr() {
            return "Create DateTime from String given as 3rd component using 2nd as format in TimeZone set as 1st." +
                    " See Java DateTimeFormatter for complete reference";
        }
    }

    public static class DateFORMAT extends Ternary<String, String, String, Object> {
        @Override
        public String call(Deque<Object> args) {
            ZoneId tz = TimeZone.getTimeZone(Evaluator.popString(args)).toZoneId();
            DateTimeFormatter dtf = new DateTimeFormatterBuilder().appendPattern(Evaluator.popString(args)).toFormatter().withZone(tz);
            return dtf.format(DateTime.parseTimestamp(Evaluator.popString(args)).toInstant());
        }

        @Override
        public String name() {
            return "DT_FORMAT";
        }

        @Override
        public String descr() {
            return "Format into String a DateTime given as 3rd argument using format from 2nd in TimeZone set as 1st." +
                    " See Java DateTimeFormatter for complete reference";
        }
    }
}
