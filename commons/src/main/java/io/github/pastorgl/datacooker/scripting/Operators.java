/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.scripting;

import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfoList;
import io.github.classgraph.ScanResult;
import io.github.pastorgl.datacooker.RegisteredPackages;
import io.github.pastorgl.datacooker.scripting.Evaluator.Binary;
import io.github.pastorgl.datacooker.scripting.Evaluator.Ternary;

import java.lang.reflect.Modifier;
import java.util.*;

public class Operators {
    public final static Map<String, Operator> OPERATORS;

    static {
        Map<String, Operator> operators = new HashMap<>();

        for (Map.Entry<String, String> pkg : RegisteredPackages.REGISTERED_PACKAGES.entrySet()) {
            try (ScanResult scanResult = new ClassGraph().acceptPackages(pkg.getKey()).scan()) {
                ClassInfoList operatorClasses = scanResult.getSubclasses(Operator.class.getTypeName());
                List<Class<?>> operatorClassRefs = operatorClasses.loadClasses();

                for (Class<?> operatorClass : operatorClassRefs) {
                    try {
                        if (!Modifier.isAbstract(operatorClass.getModifiers())) {
                            Operator operator = (Operator) operatorClass.getDeclaredConstructor().newInstance();
                            operators.put(operator.name(), operator);
                        }
                    } catch (Exception e) {
                        System.err.println("Cannot instantiate Operator class '" + operatorClass.getTypeName() + "'");
                        e.printStackTrace(System.err);
                    }
                }
            }
        }

        OPERATORS = Collections.unmodifiableMap(operators);
    }

    static Operator get(String symbol) {
        return OPERATORS.get(symbol);
    }

    // Those three are in fact peculiar language constructs, so we make sure they
    // won't go into the list of real Expression Operators. Therefore, this class
    // is not included in @RegisteredPackage, and we directly reference them in
    // the interpreter instead.
    public static Operator IN = new IN();
    public static Operator IS = new IS();
    public static Operator BETWEEN = new BETWEEN();

    public static class IN extends Operator implements Binary {
        @Override
        public int prio() {
            return 35;
        }

        @Override
        public boolean rightAssoc() {
            return true;
        }

        @Override
        public String name() {
            return "IN";
        }

        @Override
        protected Object op0(Deque<Object> args) {
            throw new RuntimeException("Direct operator IN call");
        }
    }

    public static class IS extends Operator implements Binary {
        @Override
        public int prio() {
            return 35;
        }

        @Override
        public boolean rightAssoc() {
            return true;
        }

        @Override
        public String name() {
            return "IS";
        }

        @Override
        protected boolean handleNull() {
            return true;
        }

        @Override
        protected Object op0(Deque<Object> args) {
            throw new RuntimeException("Direct operator IS call");
        }
    }

    public static class BETWEEN extends Operator implements Ternary {
        @Override
        public int prio() {
            return 35;
        }

        @Override
        public boolean rightAssoc() {
            return true;
        }

        @Override
        public String name() {
            return "BETWEEN";
        }

        @Override
        protected Object op0(Deque<Object> args) {
            throw new RuntimeException("Direct operator BETWEEN call");
        }
    }
}
