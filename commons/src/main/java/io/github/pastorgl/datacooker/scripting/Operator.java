/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.scripting;

import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.data.AttrGetter;
import org.apache.commons.codec.binary.Hex;

import java.security.MessageDigest;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public enum Operator {
    TERNARY1(":", 0, 2, true, true) {
        @Override
        protected Object op0(Deque<Object> args) {
            Object a = args.pop();
            Object b = args.pop();
            return (a == null) ? b : a;
        }
    },
    DEFAULT("DEFAULT", 0, 2, true, true) {
        @Override
        protected Object op0(Deque<Object> args) {
            return TERNARY1.op0(args);
        }
    },
    TERNARY2("?", 5) {
        @Override
        protected Object op0(Deque<Object> args) {
            boolean a = popBoolean(args);
            Object b = args.pop();
            return a ? b : null;
        }
    },

    OR("OR", 10, 2, false, true) {
        @Override
        protected Object op0(Deque<Object> args) {
            boolean y = peekNull(args);
            boolean a = !y && popBoolean(args);
            boolean z = peekNull(args);
            boolean b = !z && popBoolean(args);
            return (y && z) ? null : (a || b);
        }
    },
    XOR("XOR", 10, 2, false, true) {
        @Override
        protected Object op0(Deque<Object> args) {
            boolean y = peekNull(args);
            boolean a = !y && popBoolean(args);
            boolean z = peekNull(args);
            boolean b = !z && popBoolean(args);
            return (y && z) ? null : (a != b);
        }
    },
    AND("AND", 20) {
        @Override
        protected Object op0(Deque<Object> args) {
            boolean a = popBoolean(args);
            boolean b = popBoolean(args);
            return a && b;
        }
    },
    NOT("NOT", 30, 1, true, true) {
        @Override
        protected Object op0(Deque<Object> args) {
            boolean a = peekNull(args);
            return a ? null : !Operator.popBoolean(args);
        }
    },
    RANDOM("RANDOM", 30, 1, true, false) {
        @Override
        protected Object op0(Deque<Object> args) {
            int a = popInt(args);
            if (a == 0) {
                return 0;
            }
            return (a < 0)
                    ? -new Random().nextInt(-a)
                    : new Random().nextInt(a);
        }
    },

    IN("IN", 35, 2, true, false) {
        @Override
        protected Object op0(Deque<Object> args) {
            throw new RuntimeException("Direct operator IN call");
        }
    },
    IS("IS", 35, 2, true, true) {
        @Override
        protected Object op0(Deque<Object> args) {
            throw new RuntimeException("Direct operator IS call");
        }
    },
    BETWEEN("BETWEEN", 35, 3, true, false) {
        @Override
        protected Object op0(Deque<Object> args) {
            throw new RuntimeException("Direct operator BETWEEN call");
        }
    },

    EQ("=", 40, 2, false, true) {
        @Override
        protected Object op0(Deque<Object> args) {
            Object a = args.pop();
            Object b = args.pop();
            if ((a == null) || (b == null)) {
                return false;
            }
            if (a instanceof Number) {
                return (double) a == Utils.parseNumber(String.valueOf(b)).doubleValue();
            }
            if (a instanceof Boolean) {
                return (boolean) a == Boolean.parseBoolean(String.valueOf(b));
            }
            return Objects.equals(a, b);
        }
    },
    EQ2("==", 40) {
        @Override
        protected Object op0(Deque<Object> args) {
            return EQ.op0(args);
        }
    },
    NEQ("!=", 40, 2, false, true) {
        @Override
        protected Object op0(Deque<Object> args) {
            Object a = args.pop();
            Object b = args.pop();
            if ((a == null) || (b == null)) {
                return false;
            }
            if (a instanceof Number) {
                return (double) a != Utils.parseNumber(String.valueOf(b)).doubleValue();
            }
            if (a instanceof Boolean) {
                return (boolean) a != Boolean.parseBoolean(String.valueOf(b));
            }
            return !Objects.equals(a, b);
        }
    },
    NE2("<>", 40) {
        @Override
        protected Object op0(Deque<Object> args) {
            return NEQ.op0(args);
        }
    },
    GE(">=", 40) {
        @Override
        protected Object op0(Deque<Object> args) {
            double a = popDouble(args);
            double b = popDouble(args);
            return a >= b;
        }
    },
    GT(">", 40) {
        @Override
        protected Object op0(Deque<Object> args) {
            double a = popDouble(args);
            double b = popDouble(args);
            return a > b;
        }
    },
    LE("<=", 40) {
        @Override
        protected Object op0(Deque<Object> args) {
            double a = popDouble(args);
            double b = popDouble(args);
            return a <= b;
        }
    },
    LT("<", 40) {
        @Override
        protected Object op0(Deque<Object> args) {
            double a = popDouble(args);
            double b = popDouble(args);
            return a < b;
        }
    },

    LIKE("LIKE", 40, 2, true, false) {
        @Override
        protected Object op0(Deque<Object> args) {
            String r = String.valueOf(args.pop());

            String pattern = String.valueOf(args.pop());
            int regexFlags = Pattern.MULTILINE;
            if (pattern.startsWith("/")) {
                int lastSlash = pattern.lastIndexOf('/');

                String patternFlags = pattern.substring(lastSlash).toLowerCase();
                regexFlags |= patternFlags.contains("i") ? Pattern.CASE_INSENSITIVE : 0;
                regexFlags |= patternFlags.contains("e") ? Pattern.DOTALL : 0;
                if (patternFlags.contains("s")) {
                    regexFlags &= ~Pattern.DOTALL;
                } else {
                    regexFlags |= Pattern.DOTALL;
                }

                pattern = pattern.substring(1, lastSlash);
            }

            final Pattern p = Pattern.compile(pattern, regexFlags);

            return (r != null) && p.matcher(r).matches();
        }
    },
    MATCH("MATCH", 40, 2, true, false) {
        @Override
        protected Object op0(Deque<Object> args) {
            return LIKE.op0(args);
        }
    },
    REGEX("REGEX", 40, 2, true, false) {
        @Override
        protected Object op0(Deque<Object> args) {
            return LIKE.op0(args);
        }
    },

    DIGEST("DIGEST", 40, 2, true, false) {
        @Override
        protected Object op0(Deque<Object> args) {
            String r = String.valueOf(args.pop());

            String digest = String.valueOf(args.pop());

            final String[] d = digest.split(" ", 2);

            MessageDigest md;
            try {
                md = (d.length > 1) ? MessageDigest.getInstance(d[1], d[0]) : MessageDigest.getInstance(d[0]);
            } catch (Exception e) {
                throw new InvalidConfigurationException("Unknown DIGEST algorithm '" + digest + "'");
            }
            return Hex.encodeHexString(md.digest(r.getBytes()));
        }
    },
    HASH("HASH", 40, 2, true, false) {
        @Override
        protected Object op0(Deque<Object> args) {
            return DIGEST.op0(args);
        }
    },
    HASHCODE("HASHCODE", 40, 1, true, true) {
        @Override
        protected Object op0(Deque<Object> args) {
            return Objects.hashCode(args.pop());
        }
    },

    BOR("|", 105) {
        @Override
        protected Object op0(Deque<Object> args) {
            long a = popLong(args);
            long b = popLong(args);
            return a | b;
        }
    },
    BXOR("#", 110) {
        @Override
        protected Object op0(Deque<Object> args) {
            long a = popLong(args);
            long b = popLong(args);
            return a ^ b;
        }
    },
    BAND("&", 115) {
        @Override
        protected Object op0(Deque<Object> args) {
            long a = popLong(args);
            long b = popLong(args);
            return a & b;
        }
    },
    BSL("<<", 120, 2, true, false) {
        @Override
        protected Object op0(Deque<Object> args) {
            long a = popLong(args);
            long b = popLong(args);
            return a << b;
        }
    },
    BSR(">>", 120, 2, true, false) {
        @Override
        protected Object op0(Deque<Object> args) {
            long a = popLong(args);
            long b = popLong(args);
            return a >> b;
        }
    },

    CAT("||", 125) {
        @Override
        protected Object op0(Deque<Object> args) {
            return args.stream().map(String::valueOf).collect(Collectors.joining());
        }
    },
    ADD("+", 125) {
        @Override
        protected Object op0(Deque<Object> args) {
            double a = popDouble(args);
            double b = popDouble(args);
            return a + b;
        }
    },
    SUB("-", 125) {
        @Override
        protected Object op0(Deque<Object> args) {
            double a = popDouble(args);
            double b = popDouble(args);
            return a - b;
        }
    },

    MUL("*", 130) {
        @Override
        protected Object op0(Deque<Object> args) {
            double a = popDouble(args);
            double b = popDouble(args);
            return a * b;
        }
    },
    DIV("/", 130) {
        @Override
        protected Object op0(Deque<Object> args) {
            double a = popDouble(args);
            double b = popDouble(args);
            return a / b;
        }
    },
    MOD("%", 130) {
        @Override
        protected Object op0(Deque<Object> args) {
            double a = popDouble(args);
            double b = popDouble(args);
            return a % b;
        }
    },

    ABS("@@", 135, 1, true, false) {
        @Override
        protected Object op0(Deque<Object> args) {
            return Math.abs(popDouble(args));
        }
    },
    EXP("^", 135, 2, true, false) {
        @Override
        protected Object op0(Deque<Object> args) {
            double a = popDouble(args);
            double b = popDouble(args);
            return Math.pow(a, b);
        }
    },

    BNOT("~", 140, 1, true, false) {
        @Override
        protected Object op0(Deque<Object> args) {
            return ~popLong(args);
        }
    };

    private static int popInt(Deque<Object> args) {
        Object a = args.pop();
        if (a instanceof Number) {
            return ((Number) a).intValue();
        }
        return Utils.parseNumber(String.valueOf(a)).intValue();
    }

    private static long popLong(Deque<Object> args) {
        Object a = args.pop();
        if (a instanceof Number) {
            return ((Number) a).longValue();
        }
        return Utils.parseNumber(String.valueOf(a)).longValue();
    }

    private static double popDouble(Deque<Object> args) {
        Object a = args.pop();
        if (a instanceof Number) {
            return ((Number) a).doubleValue();
        }
        return Utils.parseNumber(String.valueOf(a)).doubleValue();
    }

    private static boolean popBoolean(Deque<Object> args) {
        Object a = args.pop();
        if (a instanceof Boolean) {
            return (Boolean) a;
        }
        return Boolean.parseBoolean(String.valueOf(a));
    }

    private static boolean peekNull(Deque<Object> args) {
        Object z = args.peek();
        return (z == null);
    }

    private final String op;
    public final int prio;
    public int ariness = 2;
    public boolean rightAssoc = false;
    public boolean handleNull = false;

    Operator(String op, int prio) {
        this.op = op;
        this.prio = prio;
    }

    Operator(String op, int prio, int ariness, boolean rightAssoc, boolean handleNull) {
        this.op = op;
        this.prio = prio;
        this.ariness = ariness;
        this.rightAssoc = rightAssoc;
        this.handleNull = handleNull;
    }

    public static Operator get(String op) {
        for (Operator eo : Operator.values()) {
            if (eo.op.equals(op)) {
                return eo;
            }
        }

        return null;
    }

    public Object op(Deque<Object> args) {
        if (!handleNull) {
            for (Object a : args) {
                if (a == null) {
                    return null;
                }
            }
        }

        return op0(args);
    }

    protected abstract Object op0(Deque<Object> args);

    public static boolean bool(AttrGetter props, List<Expression<?>> item, VariablesContext vc) {
        if ((item == null) || item.isEmpty()) {
            return true;
        }

        Object r = eval(props, item, vc);
        if (r == null) {
            return false;
        }

        return Boolean.parseBoolean(String.valueOf(r));
    }

    public static Object eval(AttrGetter props, List<Expression<?>> item, VariablesContext vc) {
        if (item.isEmpty()) {
            return null;
        }

        Deque<Object> stack = new LinkedList<>();
        Deque<Object> top = null;
        for (Expression<?> ei : item) {
            // these all push to expression stack
            if (ei instanceof Expressions.PropItem) {
                stack.push(((Expressions.PropItem) ei).get(props));
                continue;
            }
            if (ei instanceof Expressions.VarItem) {
                stack.push(((Expressions.VarItem) ei).get(vc));
                continue;
            }
            if (ei instanceof Expressions.StringItem) {
                stack.push(((Expressions.StringItem) ei).get());
                continue;
            }
            if (ei instanceof Expressions.NumericItem) {
                stack.push(((Expressions.NumericItem) ei).get());
                continue;
            }
            if (ei instanceof Expressions.NullItem) {
                stack.push(((Expressions.NullItem) ei).get());
                continue;
            }
            if (ei instanceof Expressions.BoolItem) {
                stack.push(((Expressions.BoolItem) ei).get());
                continue;
            }
            if (ei instanceof Expressions.SetItem) {
                stack.push(((Expressions.SetItem) ei).get());
                continue;
            }
            if (ei instanceof Expressions.OpItem) {
                stack.push(((Expressions.OpItem) ei).eval(top));
                continue;
            }
            if (ei instanceof Expressions.IsExpr) {
                stack.push(((Expressions.IsExpr) ei).eval(top.pop()));
                continue;
            }
            if (ei instanceof Expressions.InExpr) {
                stack.push(((Expressions.InExpr) ei).eval(top.pop(), top.pop()));
                continue;
            }
            if (ei instanceof Expressions.BetweenExpr) {
                stack.push(((Expressions.BetweenExpr) ei).eval(top.pop()));
                continue;
            }
            // and this one pops from it
            if (ei instanceof Expressions.StackGetter) {
                top = ((Expressions.StackGetter) ei).get(stack);
                continue;
            }
        }

        if (stack.size() != 1) {
            throw new RuntimeException("Invalid SELECT item expression");
        }

        return stack.pop();
    }
}
