/**
 * Copyright (C) 2024 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.commons.operators;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.pastorgl.datacooker.data.PlainText;
import io.github.pastorgl.datacooker.data.Structured;
import io.github.pastorgl.datacooker.scripting.Evaluator;
import io.github.pastorgl.datacooker.scripting.Operator;

import java.util.Deque;
import java.util.Map;

@SuppressWarnings("unused")
public class TypeConversionOperators {
    public static class BOOL extends Operator.Unary<Boolean, Object> {
        @Override
        public int prio() {
            return 150;
        }

        @Override
        protected Boolean op0(Deque<Object> args) {
            return Evaluator.popBoolean(args);
        }

        @Override
        public String name() {
            return "BOOL";
        }

        @Override
        public String descr() {
            return "Cast to Boolean";
        }

        @Override
        public boolean rightAssoc() {
            return true;
        }
    }

    public static class BOOL2 extends BOOL {
        @Override
        public String name() {
            return "BOOLEAN";
        }

        @Override
        public String descr() {
            return "Alias of BOOL";
        }
    }

    public static class INT extends Operator.Unary<Integer, Object> {
        @Override
        public int prio() {
            return 150;
        }

        @Override
        protected Integer op0(Deque<Object> args) {
            return Evaluator.popInt(args);
        }

        @Override
        public String name() {
            return "INT";
        }

        @Override
        public String descr() {
            return "Cast to Integer";
        }

        @Override
        public boolean rightAssoc() {
            return true;
        }
    }

    public static class INT2 extends INT {
        @Override
        public String name() {
            return "INTEGER";
        }

        @Override
        public String descr() {
            return "Alias of INT";
        }
    }

    public static class LONG extends Operator.Unary<Long, Object> {
        @Override
        public int prio() {
            return 150;
        }

        @Override
        protected Long op0(Deque<Object> args) {
            return Evaluator.popLong(args);
        }

        @Override
        public String name() {
            return "LONG";
        }

        @Override
        public String descr() {
            return "Cast to Long";
        }

        @Override
        public boolean rightAssoc() {
            return true;
        }
    }

    public static class DOUBLE extends Operator.Unary<Double, Object> {
        @Override
        public int prio() {
            return 150;
        }

        @Override
        protected Double op0(Deque<Object> args) {
            return Evaluator.popDouble(args);
        }

        @Override
        public String name() {
            return "DOUBLE";
        }

        @Override
        public String descr() {
            return "Cast to Double";
        }

        @Override
        public boolean rightAssoc() {
            return true;
        }
    }

    public static class STRING extends Operator.Unary<String, Object> {
        @Override
        public int prio() {
            return 150;
        }

        @Override
        protected String op0(Deque<Object> args) {
            return Evaluator.popString(args);
        }

        @Override
        public String name() {
            return "STRING";
        }

        @Override
        public String descr() {
            return "Cast to String";
        }

        @Override
        public boolean rightAssoc() {
            return true;
        }
    }

    public static class JSON extends Operator.Unary<Structured, Object> {
        @Override
        public int prio() {
            return 150;
        }

        @Override
        protected Structured op0(Deque<Object> args) {
            try {
                Object json = args.pop();

                if (json instanceof Structured) {
                    return (Structured) json;
                } else if (json instanceof Map) {
                    return new Structured(json);
                } else {
                    ObjectMapper om = new ObjectMapper();
                    om.enable(DeserializationFeature.USE_JAVA_ARRAY_FOR_JSON_ARRAY);

                    return new Structured(om.readValue(String.valueOf(json), Object.class));
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public String name() {
            return "STRUCT";
        }

        @Override
        public String descr() {
            return "Convert JSON String or derived Object to Structured Object";
        }

        @Override
        public boolean rightAssoc() {
            return true;
        }
    }

    public static class JSON2 extends JSON {
        @Override
        public String name() {
            return "JSON";
        }

        @Override
        public String descr() {
            return "Alias of STRUCT";
        }
    }

    public static class PLAIN_TEXT extends Operator.Unary<PlainText, Object> {
        @Override
        public int prio() {
            return 150;
        }

        @Override
        protected PlainText op0(Deque<Object> args) {
            try {
                Object raw = args.pop();

                if (raw instanceof PlainText) {
                    return (PlainText) raw;
                } else if (raw instanceof byte[]) {
                    return new PlainText((byte[]) raw);
                } else {
                    return new PlainText(String.valueOf(raw));
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public String name() {
            return "PLAINTEXT";
        }

        @Override
        public String descr() {
            return "Convert a byte array or stringify any Object to PlainText representation";
        }

        @Override
        public boolean rightAssoc() {
            return true;
        }
    }

    public static class PLAIN_TEXT2 extends PLAIN_TEXT {
        @Override
        public String name() {
            return "RAW";
        }

        @Override
        public String descr() {
            return "Alias of PLAIN_TEXT";
        }
    }
}
