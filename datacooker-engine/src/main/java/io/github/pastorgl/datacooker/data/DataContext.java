/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.data;

import io.github.pastorgl.datacooker.Constants;
import io.github.pastorgl.datacooker.config.*;
import io.github.pastorgl.datacooker.data.spatial.PointEx;
import io.github.pastorgl.datacooker.data.spatial.PolygonEx;
import io.github.pastorgl.datacooker.data.spatial.SegmentedTrack;
import io.github.pastorgl.datacooker.data.spatial.TrackSegment;
import io.github.pastorgl.datacooker.metadata.Pluggable;
import io.github.pastorgl.datacooker.metadata.PluggableInfo;
import io.github.pastorgl.datacooker.metadata.Pluggables;
import io.github.pastorgl.datacooker.scripting.*;
import io.github.pastorgl.datacooker.scripting.operation.Transformer;
import org.apache.commons.collections4.map.ListOrderedMap;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.github.pastorgl.datacooker.Constants.DUAL_DS;
import static io.github.pastorgl.datacooker.Constants.METRICS_DS;
import static io.github.pastorgl.datacooker.Options.*;

@SuppressWarnings("unchecked")
public class DataContext {
    public static final List<String> METRICS_COLUMNS = Arrays.asList("_name", "_type", "_parts", "_counter", "_total", "_unique", "_average", "_median");
    public static final List<String> METRICS_DEEP = Arrays.asList("_part", "_counter", "_total", "_unique", "_average", "_median");

    protected final JavaSparkContext sparkContext;

    private static StorageLevel sl = StorageLevel.fromString(storage_level.def());
    private static int ut = usage_threshold.def();

    protected final Map<String, DataStream> store = new LinkedHashMap<>();

    public static StorageLevel storageLevel() {
        return sl;
    }

    public static int usageThreshold() {
        return ut;
    }

    public DataContext(final JavaSparkContext sparkContext) {
        this.sparkContext = sparkContext;

        store.put(METRICS_DS, new DataStreamBuilder(METRICS_DS, Collections.singletonMap(ObjLvl.VALUE, METRICS_COLUMNS))
                .generated("ANALYZE", StreamType.Columnar)
                .keyExpr("_name")
                .build(sparkContext.parallelizePairs(new ArrayList<>(), 1))
        );
        store.put(DUAL_DS, new DataStreamBuilder(DUAL_DS, Collections.singletonMap(ObjLvl.VALUE, List.of("dummy")))
                .generated("DUAL", StreamType.PlainText)
                .build(sparkContext.parallelizePairs(List.of(new Tuple2<>("x", new PlainText("x"))), 1))
        );
    }

    public void initialize(OptionsContext options) {
        String storageLevel = options.getString(storage_level.name(), storage_level.def());
        sl = StorageLevel.fromString(storageLevel);

        ut = options.getNumber(usage_threshold.name(), usage_threshold.def()).intValue();

        String logLevel = options.getString(log_level.name(), log_level.def());
        sparkContext.setLogLevel(logLevel);
    }

    private DataStream surpassUsages(String dsName) {
        DataStream ds = store.get(dsName);

        if (ds.getUsages() < ut) {
            return ds;
        }

        if (ds.rdd.getStorageLevel() == StorageLevel.NONE()) {
            ds.rdd.persist(sl);
        }
        return ds;
    }

    public DataStream get(String dsName) {
        if (store.containsKey(dsName)) {
            return surpassUsages(dsName);
        }

        throw new InvalidConfigurationException("Reference to undefined DataStream '" + dsName + "'");
    }

    public Set<String> getWildcard() {
        return store.keySet();
    }

    public List<String> getWildcard(String prefix) {
        List<String> streamNames = new ArrayList<>();
        Set<String> streams = store.keySet();

        int nl = prefix.length();
        for (String key : streams) {
            if ((key.length() > nl) && key.startsWith(prefix)) {
                streamNames.add(key);
            }
        }

        if (streamNames.isEmpty()) {
            throw new InvalidConfigurationException("Requested DataStreams by wildcard" +
                    " specification '" + prefix + " *' but found nothing");
        }

        return streamNames;
    }

    public ListOrderedMap<String, DataStream> getWildcard(String prefix, int[] partitions) {
        ListOrderedMap<String, DataStream> ret = new ListOrderedMap<>();
        for (String name : getWildcard(prefix)) {
            ret.put(name, partition(name, partitions));
        }

        return ret;
    }

    public DataStream partition(String name, int[] partitions) {
        DataStream ds = surpassUsages(name);

        if (partitions != null) {
            ds = new DataStreamBuilder(name, ds.attributes())
                    .filtered("PARTITION", ds)
                    .build(RetainerRDD.retain(ds.rdd, partitions));
        }

        return ds;
    }

    public void put(String name, DataStream ds) {
        store.put(name, ds);
    }

    public Map<String, DataStream> result() {
        return Collections.unmodifiableMap(store);
    }

    public ListOrderedMap<String, StreamInfo> createDataStreams(String adapter, String inputName, String path, boolean wildcard, Map<String, Object> params, Map<ObjLvl, List<String>> reqCols, int partCount, Partitioning partitioning) {
        try {
            PluggableInfo iaInfo = Pluggables.INPUTS.get(adapter);

            Pluggable ia = iaInfo.instance();
            ia.configure(new Configuration(iaInfo.meta.definitions, iaInfo.meta.verb, params));
            ia.initialize(new PathInput(sparkContext, path, wildcard, partCount, partitioning), new Output(inputName, reqCols));
            ia.execute();

            ListOrderedMap<String, StreamInfo> si = new ListOrderedMap<>();
            Map<String, DataStream> inputs = ia.result();
            for (Map.Entry<String, DataStream> ie : inputs.entrySet()) {
                String dsName = ie.getKey();
                if (store.containsKey(dsName)) {
                    throw new RuntimeException("DS \"" + dsName + "\" requested to CREATE already exists");
                }

                DataStream dataStream = ie.getValue();
                store.put(dsName, dataStream);

                si.put(dsName, new StreamInfo(dataStream.attributes(), dataStream.keyExpr, dataStream.rdd.getStorageLevel().description(),
                        dataStream.streamType.name(), dataStream.rdd.getNumPartitions(), dataStream.getUsages()));
            }

            return si;
        } catch (Exception e) {
            throw new InvalidConfigurationException("CREATE \"" + inputName + "\" failed with an exception", e);
        }
    }

    public void copyDataStream(String adapter, DataStream ds, String path, Map<String, Object> params, Map<ObjLvl, List<String>> filterCols) {
        try {
            PluggableInfo oaInfo = Pluggables.OUTPUTS.get(adapter);

            Pluggable oa = oaInfo.instance();
            oa.configure(new Configuration(oaInfo.meta.definitions, oaInfo.meta.verb, params));
            oa.initialize(new Input(ds), new PathOutput(sparkContext, path, filterCols));
            oa.execute();

            ds.lineage.add(new StreamLineage(ds.name, oaInfo.meta.verb, StreamOrigin.COPIED, Collections.singletonList(path)));
        } catch (Exception e) {
            throw new InvalidConfigurationException("COPY \"" + ds.name + "\" failed with an exception", e);
        }
    }

    public StreamInfo partitionDataStream(String dsName, int partCount, boolean shuffle) {
        DataStream dataStream = surpassUsages(dsName);

        int _partCount = (partCount == 0) ? dataStream.rdd.getNumPartitions() : partCount;

        JavaPairRDD<Object, DataRecord<?>> rdd;

        if (shuffle) {
            rdd = dataStream.rdd.coalesce(_partCount, true)
                    .groupByKey()
                    .mapPartitionsToPair(it -> {
                        List<Tuple2<Object, DataRecord<?>>> ret = new ArrayList<>();

                        while (it.hasNext()) {
                            Tuple2<Object, Iterable<DataRecord<?>>> rec = it.next();

                            for (DataRecord<?> r : rec._2) {
                                ret.add(new Tuple2<>(rec._1, r));
                            }
                        }

                        return ret.iterator();
                    }, true);
        } else {
            rdd = dataStream.rdd.repartition(_partCount);
        }

        dataStream = new DataStreamBuilder(dsName, dataStream.attributes())
                .altered("PARTITION", dataStream)
                .build(rdd);

        store.replace(dsName, dataStream);

        return new StreamInfo(dataStream.attributes(), dataStream.keyExpr, dataStream.rdd.getStorageLevel().description(),
                dataStream.streamType.name(), dataStream.rdd.getNumPartitions(), dataStream.getUsages());
    }

    public StreamInfo transformDataStream(PluggableInfo trInfo, String dsName,
                                          Map<ObjLvl, List<String>> newColumns, Map<String, Object> params) {
        DataStream dataStream = surpassUsages(dsName);

        try {
            Transformer tr = (Transformer) trInfo.instance();
            tr.configure(new Configuration(trInfo.meta.definitions, trInfo.meta.verb, params));

            tr.initialize(new Input(dataStream), new Output(dsName, newColumns));
            tr.execute();
            dataStream = tr.result().get(dsName);
        } catch (Exception e) {
            throw new InvalidConfigurationException("TRANSFORM \"" + dsName + "\" failed with an exception", e);
        }

        store.replace(dsName, dataStream);

        return new StreamInfo(dataStream.attributes(), dataStream.keyExpr, dataStream.rdd.getStorageLevel().description(),
                dataStream.streamType.name(), dataStream.rdd.getNumPartitions(), dataStream.getUsages());
    }

    public StreamInfo keyDataStream(String dsName,
                                    final List<Expressions.ExprItem<?>> keyExpression, String ke,
                                    VariablesContext variables) {
        DataStream dataStream = surpassUsages(dsName);

        final Broadcast<VariablesContext> _vc = sparkContext.broadcast(variables);

        JavaPairRDD<Object, DataRecord<?>> reKeyed = dataStream.rdd.mapPartitionsToPair(it -> {
            VariablesContext vc = _vc.getValue();
            List<Tuple2<Object, DataRecord<?>>> ret = new ArrayList<>();

            while (it.hasNext()) {
                Tuple2<Object, DataRecord<?>> rec = it.next();

                ret.add(new Tuple2<>(Expressions.eval(rec._1, rec._2, keyExpression, vc), rec._2));
            }

            return ret.iterator();
        }, true);

        dataStream = new DataStreamBuilder(dsName, dataStream.attributes())
                .altered("KEY", dataStream)
                .keyExpr(ke)
                .build(reKeyed);

        store.replace(dsName, dataStream);

        return new StreamInfo(dataStream.attributes(), dataStream.keyExpr, dataStream.rdd.getStorageLevel().description(),
                dataStream.streamType.name(), dataStream.rdd.getNumPartitions(), dataStream.getUsages());
    }

    public boolean has(String dsName) {
        return store.containsKey(dsName);
    }

    public DataStream fromUnion(String prefix, ListOrderedMap<String, int[]> fromParts, UnionSpec unionSpec) {
        DataStream stream0 = surpassUsages(fromParts.get(0));
        Map<ObjLvl, List<String>> attrs0 = stream0.attributes();

        int inpSize = fromParts.size();
        DataStream[] inputs = new DataStream[inpSize];
        inputs[0] = stream0;

        for (int i = 1; i < inpSize; i++) {
            DataStream streamI = partition(fromParts.get(i), fromParts.getValue(i));

            if (streamI.streamType != stream0.streamType) {
                throw new InvalidConfigurationException("Can't UNION DataStreams of different types");
            }
            if (!streamI.attributes(streamI.streamType.topLevel()).containsAll(attrs0.get(stream0.streamType.topLevel()))
                    || !attrs0.get(stream0.streamType.topLevel()).containsAll(streamI.attributes(streamI.streamType.topLevel()))) {
                throw new InvalidConfigurationException("DataStreams to UNION must have same top-level record" +
                        " attributes");
            }

            inputs[i] = streamI;
        }

        JavaPairRDD<Object, DataRecord<?>> sourceRdd = null;

        if (unionSpec == UnionSpec.CONCAT) {
            JavaPairRDD<Object, DataRecord<?>>[] rdds = new JavaPairRDD[inpSize];
            for (int i = 0; i < inpSize; i++) {
                rdds[i] = inputs[i].rdd;
            }

            sourceRdd = sparkContext.<Object, DataRecord<?>>union(rdds);
        } else {
            JavaPairRDD<Tuple2<Object, DataRecord<?>>, Integer>[] paired = new JavaPairRDD[inpSize];
            for (int i = 0; i < inpSize; i++) {
                JavaPairRDD<Object, DataRecord<?>> rddI = inputs[i].rdd;

                final Integer ii = i;
                paired[i] = rddI.mapToPair(v -> new Tuple2<>(v, ii));
            }

            JavaPairRDD<Tuple2<Object, DataRecord<?>>, Integer> union = sparkContext.<Tuple2<Object, DataRecord<?>>, Integer>union(paired);
            switch (unionSpec) {
                case XOR: {
                    sourceRdd = union
                            .groupByKey()
                            .mapValues(it -> {
                                final Set<Integer> inpSet = new HashSet<>();
                                final long[] count = new long[1];
                                it.forEach(i -> {
                                    inpSet.add(i);
                                    count[0]++;
                                });

                                if (inpSet.size() > 1) {
                                    return 0L;
                                } else {
                                    return count[0];
                                }
                            })
                            .flatMapToPair(t -> Stream.generate(() -> t._1).limit(t._2).iterator());
                    break;
                }
                case AND: {
                    sourceRdd = union
                            .groupByKey()
                            .mapValues(it -> {
                                Iterator<Integer> iter = it.iterator();
                                Set<Integer> inpSet = new HashSet<>();
                                Map<Integer, Long> counts = new HashMap<>();
                                while (iter.hasNext()) {
                                    Integer ii = iter.next();
                                    inpSet.add(ii);
                                    counts.compute(ii, (i, v) -> {
                                        if (v == null) {
                                            return 1L;
                                        }
                                        return v + 1L;
                                    });
                                }
                                if (inpSet.size() < inpSize) {
                                    return 0L;
                                } else {
                                    return counts.values().stream().mapToLong(Long::longValue).reduce(Math::min).orElse(0L);
                                }
                            })
                            .flatMapToPair(t -> Stream.generate(() -> t._1).limit(t._2).iterator());
                    break;
                }
            }
        }

        return new DataStreamBuilder((prefix != null) ? prefix : stream0.name, attrs0)
                .generated("UNION", stream0.streamType, inputs)
                .build(sourceRdd);
    }

    public DataStream fromJoin(ListOrderedMap<String, int[]> fromParts, JoinSpec joinSpec) {
        final int inpSize = fromParts.size();

        if (inpSize < 2) {
            throw new InvalidConfigurationException("JOIN requires multiple DataStreams");
        }

        DataStream[] inputs = new DataStream[inpSize];
        for (int i = 0; i < inpSize; i++) {
            inputs[i] = partition(fromParts.get(i), fromParts.getValue(i));
        }

        DataStream stream0 = inputs[0];
        DataStream streamZ = inputs[inpSize - 1];

        JavaPairRDD<Object, DataRecord<?>> sourceRdd;

        Map<ObjLvl, List<String>> resultAttrs = new HashMap<>();
        String resultName;
        StreamType resultType;

        if (joinSpec == JoinSpec.LEFT_ANTI) {
            resultAttrs.putAll(stream0.attributes());
            resultName = stream0.name;
            resultType = stream0.streamType;

            JavaPairRDD<Object, DataRecord<?>> leftInputRDD = stream0.rdd;
            for (int r = 1; r < inpSize; r++) {
                JavaPairRDD<Object, DataRecord<?>> rightInputRDD = inputs[r].rdd;

                leftInputRDD = leftInputRDD.subtractByKey(rightInputRDD);
            }

            sourceRdd = leftInputRDD;
        } else if (joinSpec == JoinSpec.RIGHT_ANTI) {
            resultAttrs.putAll(streamZ.attributes());
            resultName = streamZ.name;
            resultType = streamZ.streamType;

            JavaPairRDD<Object, DataRecord<?>> rightInputRDD = streamZ.rdd;
            for (int l = inpSize - 2; l >= 0; l--) {
                JavaPairRDD<Object, DataRecord<?>> leftInputRDD = inputs[l].rdd;

                rightInputRDD = rightInputRDD.subtractByKey(leftInputRDD);
            }

            sourceRdd = rightInputRDD;
        } else if (joinSpec == JoinSpec.RIGHT) {
            final DataRecord<?> template = streamZ.itemTemplate();
            resultName = streamZ.name;
            resultType = streamZ.streamType;

            resultAttrs.put(ObjLvl.VALUE, stream0.attributes(ObjLvl.VALUE).stream()
                    .map(e -> stream0.name + "." + e)
                    .collect(Collectors.toList()));

            final StreamType _resultType = streamZ.streamType;
            JavaPairRDD<Object, DataRecord<?>> leftInputRDD = stream0.rdd;
            for (int l = 0, r = 1; r < inpSize; l++, r++) {
                String inputL = inputs[l].name;

                DataStream streamR = inputs[r];
                String inputR = streamR.name;
                resultAttrs.get(ObjLvl.VALUE).addAll(streamR.attributes(ObjLvl.VALUE).stream()
                        .map(e -> inputR + "." + e)
                        .toList());

                final boolean first = (l == 0);
                leftInputRDD = leftInputRDD.rightOuterJoin(streamR.rdd)
                        .mapPartitionsToPair(it -> {
                            List<Tuple2<Object, DataRecord<?>>> res = new ArrayList<>();

                            while (it.hasNext()) {
                                Tuple2<Object, Tuple2<Optional<DataRecord<?>>, DataRecord<?>>> o = it.next();

                                DataRecord<?> right = o._2._2;

                                DataRecord<?> merged = (DataRecord<?>) template.clone();
                                switch (_resultType) {
                                    case Point: {
                                        if (right instanceof Geometry) {
                                            merged = new PointEx((Geometry) right);
                                        }
                                        break;
                                    }
                                    case Track: {
                                        if (right instanceof SegmentedTrack) {
                                            merged = new SegmentedTrack(((SegmentedTrack) right).geometries());
                                        }
                                        break;
                                    }
                                    case Polygon: {
                                        if (right instanceof PolygonEx) {
                                            merged = new PolygonEx((Geometry) right);
                                        }
                                        break;
                                    }
                                }

                                for (Map.Entry<String, Object> e : right.asIs().entrySet()) {
                                    merged.put(inputR + "." + e.getKey(), e.getValue());
                                }
                                DataRecord<?> left = o._2._1.orNull();
                                if (left != null) {
                                    if (first) {
                                        for (Map.Entry<String, Object> e : left.asIs().entrySet()) {
                                            merged.put(inputL + "." + e.getKey(), e.getValue());
                                        }
                                    } else {
                                        merged.put(left.asIs());
                                    }
                                }

                                res.add(new Tuple2<>(o._1, merged));
                            }

                            return res.iterator();
                        });
            }

            sourceRdd = leftInputRDD;
        } else if ((joinSpec == JoinSpec.LEFT) || (joinSpec == JoinSpec.INNER)) {
            final DataRecord<?> template = stream0.itemTemplate();
            resultName = stream0.name;
            resultType = stream0.streamType;

            resultAttrs.put(ObjLvl.VALUE, stream0.attributes(ObjLvl.VALUE).stream()
                    .map(e -> stream0.name + "." + e)
                    .collect(Collectors.toList()));

            final StreamType _resultType = stream0.streamType;
            JavaPairRDD<Object, DataRecord<?>> leftInputRDD = stream0.rdd;
            for (int l = 0, r = 1; r < inpSize; l++, r++) {
                final String inputR = inputs[r].name;
                final String inputL = inputs[l].name;

                DataStream streamR = surpassUsages(inputR);
                resultAttrs.get(ObjLvl.VALUE).addAll(streamR.attributes(ObjLvl.VALUE).stream()
                        .map(e -> inputR + "." + e)
                        .toList());

                JavaPairRDD<Object, DataRecord<?>> rddR = streamR.rdd;
                JavaPairRDD<Object, ?> partialJoin = (joinSpec == JoinSpec.LEFT)
                        ? leftInputRDD.leftOuterJoin(rddR)
                        : leftInputRDD.join(rddR);

                final boolean first = (l == 0);
                leftInputRDD = partialJoin
                        .mapPartitionsToPair(it -> {
                            List<Tuple2<Object, DataRecord<?>>> res = new ArrayList<>();

                            while (it.hasNext()) {
                                Tuple2<Object, ?> o = it.next();

                                Tuple2<DataRecord<?>, Object> v = (Tuple2<DataRecord<?>, Object>) o._2;
                                DataRecord<?> left = v._1;

                                DataRecord<?> merged;
                                switch (_resultType) {
                                    case Point: {
                                        if (left instanceof Geometry) {
                                            merged = new PointEx((Geometry) left);
                                            break;
                                        }
                                    }
                                    case Track: {
                                        if (left instanceof SegmentedTrack) {
                                            merged = new SegmentedTrack(((SegmentedTrack) left).geometries());
                                            break;
                                        }
                                    }
                                    case Polygon: {
                                        if (left instanceof PolygonEx) {
                                            merged = new PolygonEx((Geometry) left);
                                            break;
                                        }
                                    }
                                    default: {
                                        merged = (DataRecord<?>) template.clone();
                                    }
                                }

                                if (first) {
                                    for (Map.Entry<String, Object> e : left.asIs().entrySet()) {
                                        merged.put(inputL + "." + e.getKey(), e.getValue());
                                    }
                                } else {
                                    merged.put(left.asIs());
                                }

                                DataRecord<?> right = (v._2 instanceof Optional) ? (DataRecord<?>) ((Optional<?>) v._2).orNull() : (DataRecord<?>) v._2;
                                if (right != null) {
                                    for (Map.Entry<String, Object> e : right.asIs().entrySet()) {
                                        merged.put(inputR + "." + e.getKey(), e.getValue());
                                    }
                                }

                                res.add(new Tuple2<>(o._1, merged));
                            }

                            return res.iterator();
                        });
            }

            sourceRdd = leftInputRDD;
        } else { // OUTER
            final DataRecord<?> template = stream0.itemTemplate();
            resultName = stream0.name;
            resultType = stream0.streamType;

            resultAttrs.put(ObjLvl.VALUE, stream0.attributes(ObjLvl.VALUE).stream()
                    .map(e -> stream0.name + "." + e)
                    .collect(Collectors.toList()));

            final StreamType _resultType = stream0.streamType;
            JavaPairRDD<Object, DataRecord<?>> leftInputRDD = stream0.rdd;
            for (int l = 0, r = 1; r < inpSize; l++, r++) {
                final String inputR = inputs[r].name;
                final String inputL = inputs[l].name;

                DataStream streamR = surpassUsages(inputR);
                resultAttrs.get(ObjLvl.VALUE).addAll(streamR.attributes(ObjLvl.VALUE).stream()
                        .map(e -> inputR + "." + e)
                        .toList());

                final boolean first = (l == 0);
                leftInputRDD = leftInputRDD.fullOuterJoin(streamR.rdd)
                        .mapPartitionsToPair(it -> {
                            List<Tuple2<Object, DataRecord<?>>> res = new ArrayList<>();

                            while (it.hasNext()) {
                                Tuple2<Object, Tuple2<Optional<DataRecord<?>>, Optional<DataRecord<?>>>> o = it.next();

                                DataRecord<?> left = o._2._1.orNull();
                                DataRecord<?> right = o._2._2.orNull();

                                DataRecord<?> merged = (DataRecord<?>) template.clone();
                                switch (_resultType) {
                                    case Point: {
                                        if (left instanceof Geometry) {
                                            merged = new PointEx((Geometry) left);
                                        } else if (right instanceof Geometry) {
                                            merged = new PointEx((Geometry) right);
                                        }
                                        break;
                                    }
                                    case Track: {
                                        if (left instanceof SegmentedTrack) {
                                            merged = new SegmentedTrack(((SegmentedTrack) left).geometries());
                                        } else if (right instanceof SegmentedTrack) {
                                            merged = new SegmentedTrack(((SegmentedTrack) right).geometries());
                                        }
                                        break;
                                    }
                                    case Polygon: {
                                        if (left instanceof PolygonEx) {
                                            merged = new PolygonEx((Geometry) left);
                                        } else if (right instanceof PolygonEx) {
                                            merged = new PolygonEx((Geometry) right);
                                        }
                                        break;
                                    }
                                }

                                if (left != null) {
                                    if (first) {
                                        for (Map.Entry<String, Object> e : left.asIs().entrySet()) {
                                            merged.put(inputL + "." + e.getKey(), e.getValue());
                                        }
                                    } else {
                                        merged.put(left.asIs());
                                    }
                                }
                                if (right != null) {
                                    for (Map.Entry<String, Object> e : right.asIs().entrySet()) {
                                        merged.put(inputR + "." + e.getKey(), e.getValue());
                                    }
                                }
                                res.add(new Tuple2<>(o._1, merged));
                            }

                            return res.iterator();
                        });
            }

            sourceRdd = leftInputRDD;
        }

        return new DataStreamBuilder(resultName, resultAttrs)
                .generated("JOIN", resultType, inputs)
                .build(sourceRdd);
    }

    public DataStream select(
            DataStream inputDs, String intoName,
            boolean distinct, // DISTINCT
            final boolean star, List<SelectItem> items, // * or aliases
            WhereItem whereItem, // WHERE
            Long limitRecords, Double limitPercent, // LIMIT
            VariablesContext variables
    ) {
        StreamType resultType = inputDs.streamType;

        Map<ObjLvl, List<String>> resultColumns;
        if (star) {
            resultColumns = inputDs.attributes();
        } else {
            resultColumns = new HashMap<>();
            for (SelectItem item : items) {
                resultColumns.compute(item.category, (k, v) -> {
                    if (v == null) {
                        v = new ArrayList<>();
                    }
                    v.add(item.alias);
                    return v;
                });
            }
        }

        JavaPairRDD<Object, DataRecord<?>> sourceRdd = inputDs.rdd;

        JavaPairRDD<Object, DataRecord<?>> output;
        if (star && (whereItem.expression == null)) {
            output = sourceRdd;
        } else {
            final List<SelectItem> _what = items;
            final WhereItem _where = whereItem;
            final Broadcast<VariablesContext> _vc = sparkContext.broadcast(variables);
            final StreamType _resultType = resultType;
            final DataRecord<?> _template = inputDs.itemTemplate();

            final int size = _what.size();
            final List<String> _columns = _what.stream().map(si -> si.alias).toList();

            switch (resultType) {
                case Columnar:
                case Structured:
                case Point:
                case Polygon: {
                    output = sourceRdd.mapPartitionsToPair(it -> {
                        VariablesContext vc = _vc.getValue();
                        List<Tuple2<Object, DataRecord<?>>> ret = new ArrayList<>();

                        while (it.hasNext()) {
                            Tuple2<Object, DataRecord<?>> rec = it.next();

                            if (Expressions.bool(rec._1, rec._2, _where.expression, vc)) {
                                if (star) {
                                    ret.add(rec);
                                } else {
                                    DataRecord<?> res;
                                    if (_resultType == StreamType.Point) {
                                        res = new PointEx((Geometry) rec._2);
                                    } else if (_resultType == StreamType.Polygon) {
                                        res = new PolygonEx((PolygonEx) rec._2);
                                    } else {
                                        res = (DataRecord<?>) _template.clone();
                                    }

                                    for (int i = 0; i < size; i++) {
                                        Object value = Expressions.eval(rec._1, rec._2, _what.get(i).expression, vc);
                                        if ((value != null) && value.getClass().isArray()) {
                                            Object[] arr = (Object[]) value;
                                            for (int j = 0; j < arr.length; j++) {
                                                res.put(_columns.get(i) + j, arr[j]);
                                            }
                                        } else {
                                            res.put(_columns.get(i), value);
                                        }
                                    }

                                    ret.add(new Tuple2<>(rec._1, res));
                                }
                            }
                        }

                        return ret.iterator();
                    });
                    break;
                }
                case Track: {
                    final boolean _qTrack = ObjLvl.TRACK.equals(whereItem.category) || ObjLvl.VALUE.equals(whereItem.category);
                    final boolean _qSegment = ObjLvl.SEGMENT.equals(whereItem.category);
                    final boolean _qPoint = ObjLvl.POINT.equals(whereItem.category);

                    output = sourceRdd.mapPartitionsToPair(it -> {
                        VariablesContext vc = _vc.getValue();
                        List<Tuple2<Object, DataRecord<?>>> ret = new ArrayList<>();

                        while (it.hasNext()) {
                            Tuple2<Object, DataRecord<?>> next = it.next();

                            SegmentedTrack st = (SegmentedTrack) next._2;
                            if (_qTrack && !Expressions.bool(next._1, st, _where.expression, vc)) {
                                continue;
                            }

                            Map<String, Object> trackProps;
                            if (star) {
                                trackProps = st.asIs();
                            } else {
                                trackProps = new HashMap<>();
                                for (int i = 0; i < size; i++) {
                                    SelectItem selectItem = _what.get(i);

                                    if (ObjLvl.TRACK.equals(selectItem.category)) {
                                        Object value = Expressions.eval(next._1, st, selectItem.expression, vc);
                                        if ((value != null) && value.getClass().isArray()) {
                                            Object[] arr = (Object[]) value;
                                            for (int j = 0; j < arr.length; j++) {
                                                trackProps.put(_columns.get(i) + j, arr[j]);
                                            }
                                        } else {
                                            trackProps.put(_columns.get(i), value);
                                        }
                                    }
                                }
                            }

                            Geometry[] segments;
                            if (_qSegment) {
                                List<Geometry> segList = new ArrayList<>();
                                for (Geometry g : st) {
                                    if (Expressions.bool(next._1, (TrackSegment) g, _where.expression, vc)) {
                                        segList.add(g);
                                    }
                                }
                                segments = segList.toArray(new Geometry[0]);
                            } else {
                                segments = st.geometries();
                            }

                            for (int j = segments.length - 1; j >= 0; j--) {
                                TrackSegment g = (TrackSegment) segments[j];

                                Map<String, Object> segProps;
                                if (star) {
                                    segProps = g.asIs();
                                } else {
                                    segProps = new HashMap<>();
                                    for (int i = 0; i < size; i++) {
                                        SelectItem selectItem = _what.get(i);

                                        if (ObjLvl.SEGMENT.equals(selectItem.category)) {
                                            Object value = Expressions.eval(next._1, g, selectItem.expression, vc);
                                            if ((value != null) && value.getClass().isArray()) {
                                                Object[] arr = (Object[]) value;
                                                for (int k = 0; k < arr.length; k++) {
                                                    segProps.put(_columns.get(i) + k, arr[k]);
                                                }
                                            } else {
                                                segProps.put(_columns.get(i), value);
                                            }
                                        }
                                    }
                                }

                                TrackSegment seg = new TrackSegment(g.geometries());
                                seg.put(segProps);
                                segments[j] = seg;
                            }

                            if (_qPoint) {
                                List<Geometry> pSegs = new ArrayList<>();
                                for (Geometry g : segments) {
                                    TrackSegment seg = (TrackSegment) g;

                                    List<Geometry> points = new ArrayList<>();
                                    for (Geometry gg : seg) {
                                        if (Expressions.bool(next._1, (PointEx) gg, _where.expression, vc)) {
                                            points.add(gg);
                                        }
                                    }

                                    if (!points.isEmpty()) {
                                        TrackSegment pSeg = new TrackSegment(points.toArray(new Geometry[0]));
                                        pSeg.put(seg.asIs());
                                        pSegs.add(pSeg);
                                    }
                                }

                                segments = pSegs.toArray(new Geometry[0]);
                            }

                            for (int k = segments.length - 1; k >= 0; k--) {
                                TrackSegment g = (TrackSegment) segments[k];
                                Map<String, Object> segProps = g.asIs();

                                Geometry[] points = g.geometries();
                                for (int j = points.length - 1; j >= 0; j--) {
                                    PointEx gg = (PointEx) points[j];

                                    Map<String, Object> pointProps;
                                    if (star) {
                                        pointProps = gg.asIs();
                                    } else {
                                        pointProps = new HashMap<>();
                                        for (int i = 0; i < size; i++) {
                                            SelectItem selectItem = _what.get(i);

                                            if (ObjLvl.POINT.equals(selectItem.category)) {
                                                Object value = Expressions.eval(next._1, gg, selectItem.expression, vc);
                                                if ((value != null) && value.getClass().isArray()) {
                                                    Object[] arr = (Object[]) value;
                                                    for (int l = 0; l < arr.length; l++) {
                                                        pointProps.put(_columns.get(i) + l, arr[l]);
                                                    }
                                                } else {
                                                    pointProps.put(_columns.get(i), value);
                                                }
                                            }
                                        }
                                    }

                                    PointEx point = new PointEx(gg);
                                    point.put(pointProps);

                                    points[j] = point;
                                }

                                TrackSegment seg = new TrackSegment(points);
                                seg.put(segProps);
                                segments[k] = seg;
                            }

                            if (segments.length > 0) {
                                SegmentedTrack rst = new SegmentedTrack(segments);
                                rst.put(trackProps);
                                ret.add(new Tuple2<>(next._1, rst));
                            }
                        }

                        return ret.iterator();
                    });
                    break;
                }
                default: {
                    output = sourceRdd;
                }
            }
        }

        if (distinct) {
            output = output.distinct();
        }

        if (limitRecords != null) {
            output = output.sample(false, limitRecords.doubleValue() / output.count());
        }
        if (limitPercent != null) {
            output = output.sample(false, limitPercent);
        }

        return new DataStreamBuilder(intoName, resultColumns)
                .generated("SELECT", resultType, inputDs)
                .build(output);
    }

    public Collection<?> subQuery(
            DataStream input,
            boolean distinct,
            List<Expressions.ExprItem<?>> item,
            List<Expressions.ExprItem<?>> query,
            Long limitRecords, Double limitPercent,
            VariablesContext variables
    ) {
        final List<Expressions.ExprItem<?>> _what = item;
        final List<Expressions.ExprItem<?>> _query = query;
        final Broadcast<VariablesContext> _vc = sparkContext.broadcast(variables);

        JavaRDD<Object> output = input.rdd
                .mapPartitions(it -> {
                    VariablesContext vc = _vc.getValue();
                    List<Object> ret = new ArrayList<>();

                    while (it.hasNext()) {
                        Tuple2<Object, DataRecord<?>> rec = it.next();

                        if (Expressions.bool(rec._1, rec._2, _query, vc)) {
                            ret.add(Expressions.eval(rec._1, rec._2, _what, vc));
                        }
                    }

                    return ret.iterator();
                });

        if (distinct) {
            output = output.distinct();
        }

        if (limitRecords != null) {
            output = output.sample(false, limitRecords.doubleValue() / output.count());
        }
        if (limitPercent != null) {
            output = output.sample(false, limitPercent);
        }

        return output.collect();
    }

    public void analyze(Map<String, DataStream> dataStreams, List<Expressions.ExprItem<?>> keyExpession, String ke, boolean deep,
                        VariablesContext variables) {
        DataStream _metrics = store.get(METRICS_DS);
        List<Tuple2<Object, DataRecord<?>>> metricsList = new ArrayList<>(_metrics.rdd.collect());

        final Broadcast<VariablesContext> _vc = sparkContext.broadcast(variables);

        final String keyExpr = keyExpession.isEmpty() ? "REC_KEY()" : ke;

        for (Map.Entry<String, DataStream> e : dataStreams.entrySet()) {
            String dsName = e.getKey();
            DataStream ds = e.getValue();

            JavaPairRDD<Object, Object> rdd2 = ds.rdd.mapPartitionsToPair(it -> {
                VariablesContext vc = _vc.getValue();

                List<Tuple2<Object, Object>> ret = new ArrayList<>();
                while (it.hasNext()) {
                    Tuple2<Object, DataRecord<?>> r = it.next();

                    Object id;
                    if (keyExpession.isEmpty()) {
                        id = r._1;
                    } else {
                        id = Expressions.eval(r._1, r._2, keyExpession, vc);
                    }

                    ret.add(new Tuple2<>(id, null));
                }

                return ret.iterator();
            });

            List<Long> counts = rdd2
                    .aggregateByKey(0L, (c, v) -> c + 1L, Long::sum)
                    .values()
                    .sortBy(t -> t, true, 1)
                    .collect();

            int unique = counts.size();
            long total = counts.stream().reduce(Long::sum).orElse(0L);
            double average = (unique == 0) ? 0.D : ((double) total / unique);
            double median = 0.D;
            if (unique != 0) {
                int m = (unique <= 2) ? 0 : (unique >> 1);
                median = ((unique % 2) == 0) ? (counts.get(m) + counts.get(m + 1)) / 2.D : counts.get(m).doubleValue();
            }

            final int numParts = ds.rdd.getNumPartitions();
            Columnar rec = new Columnar(METRICS_COLUMNS, new Object[]{dsName, ds.streamType.name(), numParts,
                    keyExpr, total, unique, average, median});
            metricsList.add(new Tuple2<>(dsName, rec));

            if (deep) {
                String name = METRICS_DS + Constants.UNDERSCORE + dsName;
                JavaPairRDD<Object, DataRecord<?>> empties = sparkContext.parallelizePairs(IntStream.range(0, numParts)
                        .mapToObj(p -> new Tuple2<Object, DataRecord<?>>(p, new Columnar(METRICS_DEEP, new Object[]{
                                p, keyExpr, 0L, 0, 0.D, 0.D
                        }))).toList(), 1);

                JavaPairRDD<Object, DataRecord<?>> deepMetrics = ds.rdd.mapPartitionsWithIndex((idx, it) -> {
                            VariablesContext vc = _vc.getValue();

                            long t = 0L;
                            HashMap<Object, Long> ids = new HashMap<>();

                            while (it.hasNext()) {
                                t++;

                                Tuple2<Object, DataRecord<?>> r = it.next();

                                Object id;
                                if (keyExpession.isEmpty()) {
                                    id = r._1;
                                } else {
                                    id = Expressions.eval(r._1, r._2, keyExpession, vc);
                                }

                                ids.compute(id, (k, v) -> v == null ? 1L : v + 1L);
                            }

                            int u = ids.size();
                            double m = 0.D;
                            if (u != 0) {
                                ArrayList<Long> ac = new ArrayList<>(ids.values());
                                Collections.sort(ac);
                                int mi = (u <= 2) ? 0 : (u >> 1);
                                m = ((u % 2) == 0) ? (ac.get(mi) + ac.get(mi + 1)) / 2.D : ac.get(mi).doubleValue();
                            }

                            List<Tuple2<Object, DataRecord<?>>> ret = Collections.singletonList(new Tuple2<>(idx, new Columnar(METRICS_DEEP, new Object[]{
                                    idx, keyExpr, t, u, (u == 0) ? 0.D : ((double) t / u), m
                            })));

                            return ret.iterator();
                        }, true)
                        .repartition(1)
                        .mapToPair(t -> t)
                        .rightOuterJoin(empties)
                        .sortByKey()
                        .mapValues(w -> w._1.orElse(w._2));
                deepMetrics = deepMetrics.persist(sl);

                put(name, new DataStreamBuilder(name, Collections.singletonMap(ObjLvl.VALUE, METRICS_DEEP))
                        .generated("ANALYZE PARTITION", StreamType.Columnar)
                        .keyExpr(keyExpr)
                        .build(deepMetrics));
            }
        }

        put(METRICS_DS, new DataStreamBuilder(METRICS_DS, Collections.singletonMap(ObjLvl.VALUE, METRICS_COLUMNS))
                .generated("ANALYZE", StreamType.Columnar, _metrics)
                .build(sparkContext.parallelizePairs(metricsList, 1).persist(sl))
        );
    }

    public StreamInfo persist(String dsName) {
        if (dsName.startsWith(METRICS_DS) || DUAL_DS.equals(dsName)) {
            return streamInfo(dsName);
        }

        DataStream ds = surpassUsages(dsName);
        if (ds.rdd.getStorageLevel() == StorageLevel.NONE()) {
            ds.rdd.persist(storageLevel());
        }

        return streamInfo(dsName);
    }

    public void renounce(String dsName) {
        if (dsName.startsWith(METRICS_DS) || DUAL_DS.equals(dsName)) {
            return;
        }

        store.remove(dsName);
    }

    public StreamInfo streamInfo(String dsName) {
        DataStream ds = surpassUsages(dsName);

        return new StreamInfo(ds.attributes(), ds.keyExpr, ds.rdd.getStorageLevel().description(),
                ds.streamType.name(), ds.rdd.getNumPartitions(), ds.getUsages());
    }
}
