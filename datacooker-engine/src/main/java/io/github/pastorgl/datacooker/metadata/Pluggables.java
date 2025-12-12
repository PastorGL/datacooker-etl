/**
 * Copyright (C) 2025 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.metadata;

import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfoList;
import io.github.classgraph.ScanResult;
import io.github.pastorgl.datacooker.PackageInfo;
import io.github.pastorgl.datacooker.RegisteredPackages;
import io.github.pastorgl.datacooker.scripting.Function;
import io.github.pastorgl.datacooker.scripting.Operator;
import org.apache.commons.collections4.map.ListOrderedMap;

import java.lang.reflect.Modifier;
import java.util.*;
import java.util.stream.Collectors;

public class Pluggables {
    public final static Map<String, OperatorInfo> OPERATORS;
    public final static Map<String, FunctionInfo> FUNCTIONS;
    public final static Map<String, PluggableInfo> INPUTS;
    public final static Map<String, PluggableInfo> OUTPUTS;
    public final static Map<String, PluggableInfo> TRANSFORMS;
    public final static Map<String, PluggableInfo> OPERATIONS;

    static {
        Map<String, OperatorInfo> allOperators = new HashMap<>();
        Map<String, FunctionInfo> allFunctions = new TreeMap<>();
        Map<String, PluggableInfo> allInputs = new TreeMap<>();
        Map<String, PluggableInfo> allOutputs = new TreeMap<>();
        Map<String, PluggableInfo> allTransforms = new TreeMap<>();
        Map<String, PluggableInfo> allOperations = new TreeMap<>();

        for (Map.Entry<String, PackageInfo> pkg : RegisteredPackages.REGISTERED_PACKAGES.entrySet()) {
            Map<String, OperatorInfo> operators = new HashMap<>();
            Map<String, FunctionInfo> functions = new TreeMap<>();
            List<PluggableInfo> pluggables = new ArrayList<>();

            try (ScanResult scanResult = new ClassGraph().acceptPackages(pkg.getKey()).scan()) {
                ClassInfoList operatorClasses = scanResult.getSubclasses(Operator.class.getTypeName());
                List<Class<?>> operatorClassRefs = operatorClasses.loadClasses();

                for (Class<?> operatorClass : operatorClassRefs) {
                    try {
                        if (!Modifier.isAbstract(operatorClass.getModifiers())) {
                            Operator<?> operator = (Operator<?>) operatorClass.getDeclaredConstructor().newInstance();

                            operators.put(operator.name(), new OperatorInfo(operator));
                        }
                    } catch (Exception e) {
                        System.err.println("Cannot instantiate Operator class '" + operatorClass.getTypeName() + "'");
                        e.printStackTrace(System.err);
                    }
                }

                ClassInfoList functionClasses = scanResult.getSubclasses(Function.class.getTypeName());
                List<Class<?>> functionClassRefs = functionClasses.loadClasses();

                for (Class<?> funcClass : functionClassRefs) {
                    try {
                        if (!Modifier.isAbstract(funcClass.getModifiers())) {
                            Function<?> func = (Function<?>) funcClass.getDeclaredConstructor().newInstance();
                            functions.put(func.name(), new FunctionInfo(func));
                        }
                    } catch (Exception e) {
                        System.err.println("Cannot instantiate Function class '" + funcClass.getTypeName() + "'");
                        e.printStackTrace(System.err);
                    }
                }

                ClassInfoList pluggableClasses = scanResult.getSubclasses(Pluggable.class.getTypeName());
                List<Class<?>> classRefs = pluggableClasses.loadClasses();

                for (Class<?> pClass : classRefs) {
                    try {
                        if (!Modifier.isAbstract(pClass.getModifiers())) {
                            PluggableMeta meta = ((Pluggable<?, ?>) pClass.getDeclaredConstructor().newInstance()).meta();

                            PluggableInfo pi = new PluggableInfo(meta, (Class<Pluggable<?, ?>>) pClass);
                            pluggables.add(pi);

                            if (meta.execFlag(ExecFlag.INPUT)) {
                                allInputs.put(meta.verb, pi);
                            }
                            if (meta.execFlag(ExecFlag.OUTPUT)) {
                                allOutputs.put(meta.verb, pi);
                            }
                            if (meta.execFlag(ExecFlag.TRANSFORM)) {
                                allTransforms.put(meta.verb, pi);
                            }
                            if (meta.execFlag(ExecFlag.OPERATION)) {
                                allOperations.put(meta.verb, pi);
                            }
                        }
                    } catch (Exception e) {
                        System.err.println("Cannot instantiate Pluggable class '" + pClass.getTypeName() + "'");
                        e.printStackTrace(System.err);
                        System.exit(8);
                    }
                }
            }

            pkg.getValue().operators.putAll((Map<? extends String, ? extends OperatorInfo>) operators.entrySet().stream()
                    .sorted(Comparator.comparingInt(o -> -o.getValue().priority))
                    .collect(Collectors.toMap(
                            Map.Entry::getKey, Map.Entry::getValue,
                            (oldValue, newValue) -> oldValue, LinkedHashMap::new))
            );
            allOperators.putAll(operators);

            pkg.getValue().functions.putAll(functions);
            allFunctions.putAll(functions);

            pluggables.sort(Comparator.comparing(e -> e.meta.verb));
            pkg.getValue().pluggables.addAll(pluggables);
        }

        if (allInputs.isEmpty()) {
            System.err.println("There are no available Input Adapters in the classpath. Won't continue");
            System.exit(5);
        }
        if (allOutputs.isEmpty()) {
            System.err.println("There are no available Output Adapters in the classpath. Won't continue");
            System.exit(6);
        }
        if (allTransforms.isEmpty()) {
            System.err.println("There are no available Transforms in the classpath. Won't continue");
            System.exit(7);
        }
        if (allOperations.isEmpty()) {
            System.err.println("There are no available Operations in the classpath. Won't continue");
            System.exit(8);
        }

        OPERATORS = Collections.unmodifiableMap(allOperators.entrySet()
                .stream()
                .sorted(Comparator.comparingInt(o -> -o.getValue().priority))
                .collect(Collectors.toMap(
                        Map.Entry::getKey, Map.Entry::getValue,
                        (oldValue, newValue) -> oldValue, LinkedHashMap::new)));
        FUNCTIONS = Collections.unmodifiableMap(allFunctions);
        INPUTS = Collections.unmodifiableMap(allInputs);
        OUTPUTS = Collections.unmodifiableMap(allOutputs);
        TRANSFORMS = Collections.unmodifiableMap(allTransforms);
        OPERATIONS = Collections.unmodifiableMap(allOperations);
    }

    public static Map<String, Integer> load() {
        Map<String, Integer> ret = new ListOrderedMap<>();
        ret.put("Registered Packages", RegisteredPackages.REGISTERED_PACKAGES.size());
        ret.put("TDL Expression Operators", OPERATORS.size());
        ret.put("TDL Expression Functions", FUNCTIONS.size());
        ret.put("Input Adapters", INPUTS.size());
        ret.put("Output Adapters", OUTPUTS.size());
        ret.put("Transforms", TRANSFORMS.size());
        ret.put("Operations", OPERATIONS.size());

        return ret;
    }
}
