/**
 * Copyright (C) 2024 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.datetime;

import io.github.pastorgl.datacooker.data.DateTime;
import io.github.pastorgl.datacooker.scripting.Evaluator.Binary;
import io.github.pastorgl.datacooker.scripting.Evaluator.Ternary;
import io.github.pastorgl.datacooker.scripting.Evaluator.Unary;
import io.github.pastorgl.datacooker.scripting.Evaluator;
import io.github.pastorgl.datacooker.scripting.Function;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Deque;
import java.util.TimeZone;

@SuppressWarnings("unused")
public class DateTimeFunctions {
    public static class DateBefore extends Function implements Binary {
        @Override
        public Object call(Deque<Object> args) {
            String end = Evaluator.popString(args);
            String testing = Evaluator.popString(args);

            return DateTime.parseTimestamp(testing).before(DateTime.parseTimestamp(end));
        }

        @Override
        public String name() {
            return "DT_BEFORE";
        }
    }

    public static class DateAfter extends Function implements Binary {
        @Override
        public Object call(Deque<Object> args) {
            String begin = Evaluator.popString(args);
            String testing = Evaluator.popString(args);

            return DateTime.parseTimestamp(testing).after(DateTime.parseTimestamp(begin));
        }

        @Override
        public String name() {
            return "DT_AFTER";
        }
    }

    public static class DateDOMTZ extends Function implements Binary {
        @Override
        public Object call(Deque<Object> args) {
            ZoneId tz = TimeZone.getTimeZone(Evaluator.popString(args)).toZoneId();
            return DateTime.parseTimestamp(Evaluator.popString(args)).toInstant().atZone(tz).getDayOfMonth();
        }

        @Override
        public String name() {
            return "DT_DAY";
        }
    }

    public static class DateDOWTZ extends Function implements Binary {
        @Override
        public Object call(Deque<Object> args) {
            ZoneId tz = TimeZone.getTimeZone(Evaluator.popString(args)).toZoneId();
            return DateTime.parseTimestamp(Evaluator.popString(args)).toInstant().atZone(tz).getDayOfWeek().getValue();
        }

        @Override
        public String name() {
            return "DT_DOW";
        }
    }

    public static class DateMONTHTZ extends Function implements Binary {
        @Override
        public Object call(Deque<Object> args) {
            ZoneId tz = TimeZone.getTimeZone(Evaluator.popString(args)).toZoneId();
            return DateTime.parseTimestamp(Evaluator.popString(args)).toInstant().atZone(tz).getMonth().getValue();
        }

        @Override
        public String name() {
            return "DT_MONTH";
        }
    }

    public static class DateYEARTZ extends Function implements Binary {
        @Override
        public Object call(Deque<Object> args) {
            ZoneId tz = TimeZone.getTimeZone(Evaluator.popString(args)).toZoneId();
            return DateTime.parseTimestamp(Evaluator.popString(args)).toInstant().atZone(tz).getYear();
        }

        @Override
        public String name() {
            return "DT_YEAR";
        }
    }

    public static class DateHOURTZ extends Function implements Binary {
        @Override
        public Object call(Deque<Object> args) {
            ZoneId tz = TimeZone.getTimeZone(Evaluator.popString(args)).toZoneId();
            return DateTime.parseTimestamp(Evaluator.popString(args)).toInstant().atZone(tz).getHour();
        }

        @Override
        public String name() {
            return "DT_HOUR";
        }
    }

    public static class DateMINUTETZ extends Function implements Binary {
        @Override
        public Object call(Deque<Object> args) {
            ZoneId tz = TimeZone.getTimeZone(Evaluator.popString(args)).toZoneId();
            return DateTime.parseTimestamp(Evaluator.popString(args)).toInstant().atZone(tz).getMinute();
        }

        @Override
        public String name() {
            return "DT_MINUTE";
        }
    }

    public static class DateSECONDTZ extends Function implements Binary {
        @Override
        public Object call(Deque<Object> args) {
            ZoneId tz = TimeZone.getTimeZone(Evaluator.popString(args)).toZoneId();
            return DateTime.parseTimestamp(Evaluator.popString(args)).toInstant().atZone(tz).getSecond();
        }

        @Override
        public String name() {
            return "DT_SECOND";
        }
    }

    public static class DateTZ extends Function implements Binary {
        @Override
        public Object call(Deque<Object> args) {
            ZoneId tz = TimeZone.getTimeZone(Evaluator.popString(args)).toZoneId();
            return DateTime.parseTimestamp(Evaluator.popString(args)).toInstant().atZone(tz).toString();
        }

        @Override
        public String name() {
            return "DT_TZ";
        }
    }

    public static class DateEPOCH extends Function implements Unary {
        @Override
        public Object call(Deque<Object> args) {
            return DateTime.parseTimestamp(Evaluator.popString(args)).getTime() / 1000L;
        }

        @Override
        public String name() {
            return "DT_EPOCH";
        }
    }

    public static class DatePARSE extends Function implements Ternary {
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
    }

    public static class DateFORMAT extends Function implements Ternary {
        @Override
        public Object call(Deque<Object> args) {
            ZoneId tz = TimeZone.getTimeZone(Evaluator.popString(args)).toZoneId();
            DateTimeFormatter dtf = new DateTimeFormatterBuilder().appendPattern(Evaluator.popString(args)).toFormatter().withZone(tz);
            return dtf.format(DateTime.parseTimestamp(Evaluator.popString(args)).toInstant());
        }

        @Override
        public String name() {
            return "DT_FORMAT";
        }
    }
}
