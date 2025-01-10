/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.data;

import io.github.pastorgl.datacooker.Constants;
import io.github.pastorgl.datacooker.config.Configuration;
import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.data.spatial.PointEx;
import io.github.pastorgl.datacooker.data.spatial.PolygonEx;
import io.github.pastorgl.datacooker.data.spatial.SegmentedTrack;
import io.github.pastorgl.datacooker.data.spatial.TrackSegment;
import io.github.pastorgl.datacooker.scripting.*;
import io.github.pastorgl.datacooker.storage.Adapters;
import io.github.pastorgl.datacooker.storage.InputAdapter;
import io.github.pastorgl.datacooker.storage.OutputAdapter;
import io.github.pastorgl.datacooker.storage.OutputAdapterInfo;
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
import java.util.stream.Stream;

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

        store.put(Constants.METRICS_DS, new DataStreamBuilder(METRICS_DS, Collections.singletonMap(ObjLvl.VALUE, METRICS_COLUMNS))
                .generated("ANALYZE", StreamType.Columnar)
                .keyExpr("_streamName")
                .build(sparkContext.parallelizePairs(new ArrayList<>(), 1))
        );
    }

    public void initialize(OptionsContext options) {
        String storageLevel = options.getString(storage_level.name(), storage_level.def());
        sl = StorageLevel.fromString(storageLevel);

        ut = options.getNumber(usage_threshold.name(), usage_threshold.def()).intValue();

        String logLevel = options.getString(log_level.name(), log_level.def());
        sparkContext.setLogLevel(logLevel);
    }

    public DataStream get(String dsName) {
        if (store.containsKey(dsName)) {
            return store.get(dsName);
        }

        throw new InvalidConfigurationException("Reference to undefined DataStream '" + dsName + "'");
    }

    public Set<String> getAll() {
        return store.keySet();
    }

    public ListOrderedMap<String, DataStream> getAll(String... templates) {
        List<String> streamNames = new ArrayList<>();
        Set<String> streams = store.keySet();

        for (String name : templates) {
            if (name.endsWith(Constants.STAR)) {
                name = name.substring(0, name.length() - 1);

                int nl = name.length();
                for (String key : streams) {
                    if ((key.length() > nl) && key.startsWith(name)) {
                        streamNames.add(key);
                    }
                }
            } else if (streams.contains(name)) {
                streamNames.add(name);
            } else {
                throw new InvalidConfigurationException("Reference to undefined DataStream '" + name + "'");
            }
        }

        if (streamNames.isEmpty()) {
            throw new InvalidConfigurationException("Requested DataStreams by wildcard" +
                    " reference '" + String.join(",", streamNames) + "' but found nothing");
        }

        ListOrderedMap<String, DataStream> ret = new ListOrderedMap<>();
        for (String name : streamNames) {
            ret.put(name, store.get(name));
        }

        return ret;
    }

    public void put(String name, DataStream ds) {
        store.put(name, ds);
    }

    public Map<String, DataStream> result() {
        return Collections.unmodifiableMap(store);
    }

    public ListOrderedMap<String, StreamInfo> createDataStreams(String adapter, String inputName, String path, Map<String, Object> params, int partCount, Partitioning partitioning) {
        try {
            InputAdapter ia = Adapters.INPUTS.get(adapter).configurable.getDeclaredConstructor().newInstance();
            Configuration config = new Configuration(ia.meta.definitions, "Input " + ia.meta.verb, params);
            ia.initialize(sparkContext, config, path);

            ListOrderedMap<String, StreamInfo> si = new ListOrderedMap<>();
            ListOrderedMap<String, DataStream> inputs = ia.load(inputName, partCount, partitioning);
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

    public void copyDataStream(String adapter, String outputName, String path, Map<String, Object> params) {
        try {
            DataStream ds = store.get(outputName);

            OutputAdapterInfo ai = Adapters.OUTPUTS.get(adapter);

            OutputAdapter oa = ai.configurable.getDeclaredConstructor().newInstance();

            oa.initialize(sparkContext, new Configuration(oa.meta.definitions, "Output " + oa.meta.verb, params), path);
            oa.save(outputName, ds);
            ds.lineage.add(new StreamLineage(outputName, oa.meta.verb, StreamOrigin.COPIED, Collections.singletonList(path)));
        } catch (Exception e) {
            throw new InvalidConfigurationException("COPY \"" + outputName + "\" failed with an exception", e);
        }
    }

    public StreamInfo alterDataStream(String dsName, StreamConverter converter, Map<ObjLvl, List<String>> newColumns,
                                      List<Expressions.ExprItem<?>> keyExpression, String ke, boolean keyAfter,
                                      int partCount, Configuration params,
                                      VariablesContext variables) {
        if (METRICS_DS.equals(dsName)) {
            return streamInfo(dsName);
        }

        DataStream dataStream = store.get(dsName);

        if (keyExpression.isEmpty()) {
            dataStream = converter.apply(dataStream, newColumns, params);
        } else {
            final Broadcast<VariablesContext> _vc = sparkContext.broadcast(variables);
            StreamKeyer keyer = (expr, ds) -> new DataStreamBuilder(dsName, ds.attributes())
                    .altered("KEY", ds)
                    .keyExpr(ke)
                    .build(ds.rdd.mapPartitionsToPair(it -> {
                                VariablesContext vc = _vc.getValue();
                                List<Tuple2<Object, DataRecord<?>>> ret = new ArrayList<>();

                                while (it.hasNext()) {
                                    Tuple2<Object, DataRecord<?>> rec = it.next();

                                    ret.add(new Tuple2<>(Expressions.evalAttr(rec._1, rec._2, expr, vc), rec._2));
                                }

                                return ret.iterator();
                            }, true)
                    );

            if (keyAfter) {
                dataStream = converter.apply(dataStream, newColumns, params);
                dataStream = keyer.apply(keyExpression, dataStream);
            } else {
                dataStream = keyer.apply(keyExpression, dataStream);
                dataStream = converter.apply(dataStream, newColumns, params);
            }
        }

        if (partCount > 0) {
            dataStream = new DataStreamBuilder(dsName, dataStream.attributes())
                    .altered("PARTITION", dataStream)
                    .build(dataStream.rdd.repartition(partCount));
        }

        store.replace(dsName, dataStream);

        return new StreamInfo(dataStream.attributes(), dataStream.keyExpr, dataStream.rdd.getStorageLevel().description(),
                dataStream.streamType.name(), dataStream.rdd.getNumPartitions(), dataStream.getUsages());
    }

    public boolean has(String dsName) {
        return store.containsKey(dsName);
    }

    public JavaPairRDD<Object, DataRecord<?>> select(
            List<String> inputs, UnionSpec unionSpec, JoinSpec joinSpec, // FROM
            final boolean star, List<SelectItem> items, // aliases or *
            WhereItem whereItem, // WHERE
            VariablesContext variables) {
        final int inpSize = inputs.size();

        if (((unionSpec != null) || (joinSpec != null)) && (inpSize < 2)) {
            throw new InvalidConfigurationException("SELECT UNION or JOIN requires multiple DataStreams");
        }

        String input0 = inputs.get(0);
        DataStream stream0 = store.get(input0);

        JavaPairRDD<Object, DataRecord<?>> sourceRdd = stream0.rdd;
        StreamType resultType = stream0.streamType;

        if (unionSpec != null) {
            for (int i = 1; i < inpSize; i++) {
                DataStream streamI = store.get(inputs.get(i));

                if (streamI.streamType != stream0.streamType) {
                    throw new InvalidConfigurationException("Can't UNION DataStreams of different types");
                }
                if (!streamI.attributes(ObjLvl.VALUE).containsAll(stream0.attributes(ObjLvl.VALUE))
                        || !stream0.attributes(ObjLvl.VALUE).containsAll(streamI.attributes(ObjLvl.VALUE))) {
                    throw new InvalidConfigurationException("DataStreams to UNION must have same top-level record" +
                            " attributes");
                }
            }

            if (unionSpec == UnionSpec.CONCAT) {
                JavaPairRDD<Object, DataRecord<?>>[] streams = new JavaPairRDD[inpSize];
                for (int i = 0; i < inpSize; i++) {
                    DataStream streamI = store.get(inputs.get(i));

                    streams[i] = streamI.rdd;
                }

                sourceRdd = sparkContext.<Object, DataRecord<?>>union(streams);
            } else {
                JavaPairRDD<Tuple2<Object, DataRecord<?>>, Integer>[] paired = new JavaPairRDD[inpSize];
                for (int i = 0; i < inpSize; i++) {
                    DataStream streamI = store.get(inputs.get(i));

                    final Integer ii = i;
                    paired[i] = streamI.rdd.mapToPair(v -> new Tuple2<>(v, ii));
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
        } else if (joinSpec != null) {
            String inputZ = inputs.get(inpSize - 1);
            DataStream streamZ = store.get(inputZ);

            if (joinSpec == JoinSpec.LEFT_ANTI) {
                JavaPairRDD<Object, DataRecord<?>> leftInputRDD = stream0.rdd;
                for (int r = 1; r < inpSize; r++) {
                    final String inputR = inputs.get(r);
                    JavaPairRDD<Object, DataRecord<?>> rightInputRDD = store.get(inputR).rdd;

                    leftInputRDD = leftInputRDD.subtractByKey(rightInputRDD);
                }

                sourceRdd = leftInputRDD;
            } else if (joinSpec == JoinSpec.RIGHT_ANTI) {
                JavaPairRDD<Object, DataRecord<?>> rightInputRDD = streamZ.rdd;
                for (int l = inpSize - 2; l >= 0; l--) {
                    final String inputL = inputs.get(l);
                    JavaPairRDD<Object, DataRecord<?>> leftInputRDD = store.get(inputL).rdd;

                    rightInputRDD = rightInputRDD.subtractByKey(leftInputRDD);
                }

                resultType = streamZ.streamType;

                sourceRdd = rightInputRDD;
            } else if (joinSpec == JoinSpec.RIGHT) {
                resultType = streamZ.streamType;

                final DataRecord<?> template = streamZ.itemTemplate();

                Map<ObjLvl, List<String>> attrs = new HashMap<>();
                attrs.put(ObjLvl.VALUE, stream0.attributes(ObjLvl.VALUE).stream()
                        .map(e -> input0 + "." + e)
                        .collect(Collectors.toList()));

                final StreamType _resultType = resultType;
                JavaPairRDD<Object, DataRecord<?>> leftInputRDD = stream0.rdd;
                for (int l = 0, r = 1; r < inpSize; l++, r++) {
                    final String inputR = inputs.get(r);
                    final String inputL = inputs.get(l);

                    DataStream streamR = store.get(inputR);
                    attrs.get(ObjLvl.VALUE).addAll(streamR.attributes(ObjLvl.VALUE).stream()
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

                Map<ObjLvl, List<String>> attrs = new HashMap<>();
                attrs.put(ObjLvl.VALUE, stream0.attributes(ObjLvl.VALUE).stream()
                        .map(e -> input0 + "." + e)
                        .collect(Collectors.toList()));

                final StreamType _resultType = resultType;
                JavaPairRDD<Object, DataRecord<?>> leftInputRDD = stream0.rdd;
                for (int l = 0, r = 1; r < inpSize; l++, r++) {
                    final String inputR = inputs.get(r);
                    final String inputL = inputs.get(l);

                    DataStream streamR = store.get(inputR);
                    attrs.get(ObjLvl.VALUE).addAll(streamR.attributes(ObjLvl.VALUE).stream()
                            .map(e -> inputR + "." + e)
                            .toList());

                    JavaPairRDD<Object, ?> partialJoin = (joinSpec == JoinSpec.LEFT)
                            ? leftInputRDD.leftOuterJoin(streamR.rdd)
                            : leftInputRDD.join(streamR.rdd);

                    final boolean first = (l == 0);
                    leftInputRDD = partialJoin
                            .mapPartitionsToPair(it -> {
                                List<Tuple2<Object, DataRecord<?>>> res = new ArrayList<>();

                                while (it.hasNext()) {
                                    Tuple2<Object, ?> o = it.next();

                                    Tuple2<DataRecord<?>, Object> v = (Tuple2<DataRecord<?>, Object>) o._2;
                                    DataRecord<?> left = v._1;

                                    DataRecord<?> merged = null;
                                    switch (_resultType) {
                                        case Point: {
                                            if (left instanceof Geometry) {
                                                merged = new PointEx((Geometry) left);
                                            }
                                            break;
                                        }
                                        case Track: {
                                            if (left instanceof SegmentedTrack) {
                                                merged = new SegmentedTrack(((SegmentedTrack) left).geometries());
                                            }
                                            break;
                                        }
                                        case Polygon: {
                                            if (left instanceof PolygonEx) {
                                                merged = new PolygonEx((Geometry) left);
                                            }
                                            break;
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

                Map<ObjLvl, List<String>> attrs = new HashMap<>();
                attrs.put(ObjLvl.VALUE, stream0.attributes(ObjLvl.VALUE).stream()
                        .map(e -> input0 + "." + e)
                        .collect(Collectors.toList()));

                final StreamType _resultType = resultType;
                JavaPairRDD<Object, DataRecord<?>> leftInputRDD = stream0.rdd;
                for (int l = 0, r = 1; r < inpSize; l++, r++) {
                    final String inputR = inputs.get(r);
                    final String inputL = inputs.get(l);

                    DataStream streamR = store.get(inputR);
                    attrs.get(ObjLvl.VALUE).addAll(streamR.attributes(ObjLvl.VALUE).stream()
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
        }

        final List<SelectItem> _what = items;
        final WhereItem _where = whereItem;
        final Broadcast<VariablesContext> _vc = sparkContext.broadcast(variables);
        final StreamType _resultType = resultType;
        final DataRecord<?> _template = stream0.itemTemplate();

        JavaPairRDD<Object, DataRecord<?>> output;

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

                        if (Expressions.boolAttr(rec._1, rec._2, _where.expression, vc)) {
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
                                    res.put(_columns.get(i), Expressions.evalAttr(rec._1, rec._2, _what.get(i).expression, vc));
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
                        if (_qTrack && !Expressions.boolAttr(next._1, st, _where.expression, vc)) {
                            continue;
                        }

                        Map<String, Object> trackProps = new HashMap<>();

                        if (!star) {
                            for (int i = 0; i < size; i++) {
                                SelectItem selectItem = _what.get(i);

                                if (ObjLvl.TRACK.equals(selectItem.category)) {
                                    trackProps.put(_columns.get(i), Expressions.evalAttr(next._1, st, selectItem.expression, vc));
                                }
                            }
                        }

                        if (trackProps.isEmpty()) {
                            trackProps = st.asIs();
                        }

                        Geometry[] segments;
                        if (_qSegment) {
                            List<Geometry> segList = new ArrayList<>();
                            for (Geometry g : st) {
                                if (Expressions.boolAttr(next._1, (TrackSegment) g, _where.expression, vc)) {
                                    segList.add(g);
                                }
                            }
                            segments = segList.toArray(new Geometry[0]);
                        } else {
                            segments = st.geometries();
                        }

                        for (int j = segments.length - 1; j >= 0; j--) {
                            TrackSegment g = (TrackSegment) segments[j];

                            Map<String, Object> segProps = new HashMap<>();

                            if (!star) {
                                for (int i = 0; i < size; i++) {
                                    SelectItem selectItem = _what.get(i);

                                    if (ObjLvl.SEGMENT.equals(selectItem.category)) {
                                        segProps.put(_columns.get(i), Expressions.evalAttr(next._1, g, selectItem.expression, vc));
                                    }
                                }
                            }

                            if (segProps.isEmpty()) {
                                segProps = g.asIs();
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
                                    if (Expressions.boolAttr(next._1, (PointEx) gg, _where.expression, vc)) {
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

                                Map<String, Object> pointProps = new HashMap<>();

                                if (!star) {
                                    for (int i = 0; i < size; i++) {
                                        SelectItem selectItem = _what.get(i);

                                        if (ObjLvl.POINT.equals(selectItem.category)) {
                                            pointProps.put(_columns.get(i), Expressions.evalAttr(next._1, gg, selectItem.expression, vc));
                                        }
                                    }
                                }

                                if (pointProps.isEmpty()) {
                                    pointProps = gg.asIs();
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

        return output;
    }

    public Collection<?> subQuery(boolean distinct, DataStream input, List<Expressions.ExprItem<?>> item, List<Expressions.ExprItem<?>> query, Double limitPercent, Long limitRecords, VariablesContext variables) {
        final List<Expressions.ExprItem<?>> _what = item;
        final List<Expressions.ExprItem<?>> _query = query;
        final Broadcast<VariablesContext> _vc = sparkContext.broadcast(variables);

        JavaRDD<Object> output = input.rdd
                .mapPartitions(it -> {
                    VariablesContext vc = _vc.getValue();
                    List<Object> ret = new ArrayList<>();

                    while (it.hasNext()) {
                        Tuple2<Object, DataRecord<?>> rec = it.next();

                        if (Expressions.boolAttr(rec._1, rec._2, _query, vc)) {
                            ret.add(Expressions.evalAttr(rec._1, rec._2, _what, vc));
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

    public void analyze(Map<String, DataStream> dataStreams, String counter, boolean deep) {
        DataStream _metrics = store.get(METRICS_DS);
        JavaPairRDD<Object, DataRecord<?>> rdd = _metrics.rdd;
        List<Tuple2<Object, DataRecord<?>>> metricsList = new ArrayList<>(rdd.collect());

        for (Map.Entry<String, DataStream> e : dataStreams.entrySet()) {
            String dsName = e.getKey();
            DataStream ds = e.getValue();

            List<String> columns = ds.attributes(ObjLvl.VALUE);

            final String _counter = columns.contains(counter) ? counter : null;

            JavaPairRDD<Object, Object> rdd2 = ds.rdd.mapPartitionsToPair(it -> {
                List<Tuple2<Object, Object>> ret = new ArrayList<>();
                while (it.hasNext()) {
                    Tuple2<Object, DataRecord<?>> r = it.next();

                    Object id;
                    if (_counter == null) {
                        id = r._1;
                    } else {
                        id = r._2.asIs(_counter);
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

            Columnar rec = new Columnar(METRICS_COLUMNS, new Object[]{dsName, ds.streamType.name(), ds.rdd.getNumPartitions(),
                    (_counter == null) ? "REC_KEY()" : _counter, total, unique, average, median});
            metricsList.add(new Tuple2<>(dsName, rec));

            if (deep) {
                String name = METRICS_DS + "_" + dsName;
                JavaPairRDD<Object, DataRecord<?>> deepMetrics = ds.rdd.mapPartitionsWithIndex((idx, it) -> {
                            long t = 0L;
                            HashMap<Object, Long> ids = new HashMap<>();

                            while (it.hasNext()) {
                                t++;

                                Tuple2<Object, DataRecord<?>> r = it.next();

                                Object id;
                                if (_counter == null) {
                                    id = r._1;
                                } else {
                                    id = r._2.asIs(_counter);
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
                                    idx, (_counter == null) ? "REC_KEY()" : _counter, t, u, (u == 0) ? 0.D : ((double) t / u), m
                            })));

                            return ret.iterator();
                        }, true)
                        .repartition(1)
                        .mapToPair(t -> t);

                put(name, new DataStreamBuilder(name, Collections.singletonMap(ObjLvl.VALUE, METRICS_DEEP))
                        .generated("ANALYZE PARTITION", StreamType.Columnar)
                        .keyExpr("PARTITION")
                        .build(deepMetrics));
            }
        }

        put(Constants.METRICS_DS, new DataStreamBuilder(METRICS_DS, Collections.singletonMap(ObjLvl.VALUE, METRICS_COLUMNS))
                .generated("ANALYZE", StreamType.Columnar, _metrics)
                .build(sparkContext.parallelizePairs(metricsList, 1))
        );
    }

    public StreamInfo persist(String dsName) {
        if (METRICS_DS.equals(dsName)) {
            return streamInfo(METRICS_DS);
        }

        store.get(dsName).surpassUsages();

        return streamInfo(dsName);
    }

    public void renounce(String dsName) {
        if (METRICS_DS.equals(dsName)) {
            return;
        }

        store.remove(dsName);
    }

    public StreamInfo streamInfo(String dsName) {
        DataStream ds = store.get(dsName);

        return new StreamInfo(ds.attributes(), ds.keyExpr, ds.rdd.getStorageLevel().description(),
                ds.streamType.name(), ds.rdd.getNumPartitions(), ds.getUsages());
    }
}
