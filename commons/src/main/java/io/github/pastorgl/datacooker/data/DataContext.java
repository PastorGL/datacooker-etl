/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
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
import scala.Function3;
import scala.Tuple2;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.github.pastorgl.datacooker.Constants.*;

@SuppressWarnings("unchecked")
public class DataContext {
    public static final List<String> METRICS_COLUMNS = Arrays.asList("_streamName", "_streamType", "_numParts", "_counterColumn", "_totalCount", "_uniqueCounters", "_counterAverage", "_counterMedian");

    public static final String OPT_STORAGE_LEVEL = "storage_level";
    public static final String OPT_USAGE_THRESHOLD = "usage_threshold";
    public static final String OPT_LOG_LEVEL = "log_level";

    protected final JavaSparkContext sparkContext;

    private StorageLevel sl = StorageLevel.MEMORY_AND_DISK();
    private int ut = 2;

    protected final HashMap<String, DataStream> store = new HashMap<>();

    protected VariablesContext options;

    public DataContext(final JavaSparkContext sparkContext) {
        this.sparkContext = sparkContext;

        store.put(Constants.METRICS_DS, new DataStream(StreamType.Columnar,
                sparkContext.parallelizePairs(new ArrayList<>(), 1),
                Collections.singletonMap(OBJLVL_VALUE, METRICS_COLUMNS)));
    }

    public void initialize(VariablesContext options) {
        this.options = options;

        String storageLevel = options.getString(OPT_STORAGE_LEVEL);
        if (storageLevel != null) {
            sl = StorageLevel.fromString(storageLevel);
        }
        Number usageThreshold = options.getNumber(OPT_USAGE_THRESHOLD);
        if (usageThreshold != null) {
            ut = usageThreshold.intValue();
        }
        String logLevel = options.getString(OPT_LOG_LEVEL, "INFO");
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

                for (String key : streams) {
                    if (key.startsWith(name)) {
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
            DataStream dataStream = store.get(name);

            if (++dataStream.usages == ut) {
                dataStream.rdd.rdd().persist(sl);
            }

            ret.put(name, dataStream);
        }

        return ret;
    }

    public void put(String name, DataStream ds) {
        store.put(name, ds);
    }

    public Map<String, DataStream> result() {
        return Collections.unmodifiableMap(store);
    }

    public int getUsageThreshold() {
        return ut;
    }

    public void createDataStreams(String adapter, String inputName, String path, Map<String, Object> params, int partCount, Partitioning partitioning) {
        try {
            InputAdapter ia = Adapters.INPUTS.get(adapter).configurable.getDeclaredConstructor().newInstance();
            Configuration config = new Configuration(ia.meta.definitions, "Input " + ia.meta.verb, params);
            ia.initialize(sparkContext, config, path);

            Map<String, DataStream> inputs = ia.load(partCount, partitioning);
            for (Map.Entry<String, DataStream> ie : inputs.entrySet()) {
                String name = ie.getKey().isEmpty() ? inputName : inputName + "/" + ie.getKey();
                ie.getValue().rdd.rdd().setName("datacooker:input:" + name);
                store.put(name, ie.getValue());
            }
        } catch (Exception e) {
            throw new InvalidConfigurationException("CREATE \"" + inputName + "\" failed with an exception", e);
        }
    }

    public void copyDataStream(String adapter, String outputName, String path, Map<String, Object> params) {
        DataStream ds = store.get(outputName);
        ds.rdd.rdd().setName("datacooker:output:" + outputName);

        try {
            OutputAdapterInfo ai = Adapters.OUTPUTS.get(adapter);

            OutputAdapter oa = ai.configurable.getDeclaredConstructor().newInstance();

            oa.initialize(sparkContext, new Configuration(oa.meta.definitions, "Output " + oa.meta.verb, params), path);
            oa.save(outputName, ds);
        } catch (Exception e) {
            throw new InvalidConfigurationException("COPY \"" + outputName + "\" failed with an exception", e);
        }
    }

    public void alterDataStream(String dsName, StreamConverter converter, Map<String, List<String>> newColumns, List<Expression<?>> keyExpression, boolean keyAfter, int partCount, Configuration params) {
        DataStream dataStream = store.get(dsName);

        if (keyExpression.isEmpty()) {
            dataStream = converter.apply(dataStream, newColumns, params);
        } else {
            Function3<List<Expression<?>>, DataStream, Accessor<? extends Record<?>>, DataStream> keyer = (expr, ds, acc) -> new DataStream(
                    ds.streamType,
                    ds.rdd.mapPartitionsToPair(it -> {
                        List<Tuple2<Object, Record<?>>> ret = new ArrayList<>();

                        while (it.hasNext()) {
                            Record<?> rec = it.next()._2();
                            AttrGetter getter = acc.getter(rec);

                            ret.add(new Tuple2<>(Operator.eval(getter, expr, null), rec));
                        }

                        return ret.iterator();
                    }, false),
                    acc.attributes()
            );

            if (keyAfter) {
                dataStream = converter.apply(dataStream, newColumns, params);
                dataStream = keyer.apply(keyExpression, dataStream, dataStream.accessor);
            } else {
                dataStream = keyer.apply(keyExpression, dataStream, dataStream.accessor);
                dataStream = converter.apply(dataStream, newColumns, params);
            }
        }

        if (partCount > 0) {
            dataStream = new DataStream(dataStream.streamType, dataStream.rdd.repartition(partCount), dataStream.accessor.attributes());
        }

        store.replace(dsName, dataStream);
    }

    public boolean has(String dsName) {
        return store.containsKey(dsName);
    }

    public JavaPairRDD<Object, Record<?>> select(boolean distinct, // DISTINCT
                                                 List<String> inputs, UnionSpec unionSpec, JoinSpec joinSpec, // FROM
                                                 final boolean star, List<SelectItem> items, // aliases or *
                                                 WhereItem whereItem, // WHERE
                                                 Double limitPercent, Long limitRecords, // LIMIT
                                                 VariablesContext variables) {
        final int inpSize = inputs.size();

        if (((unionSpec != null) || (joinSpec != null)) && (inpSize < 2)) {
            throw new InvalidConfigurationException("SELECT UNION or JOIN requires multiple DataStreams");
        }

        String input0 = inputs.get(0);
        DataStream stream0 = store.get(input0);

        JavaPairRDD<Object, Record<?>> sourceRdd = stream0.rdd;
        Accessor<? extends Record<?>> resultAccessor = stream0.accessor;
        StreamType resultType = stream0.streamType;

        if (unionSpec != null) {
            for (int i = 1; i < inpSize; i++) {
                DataStream streamI = store.get(inputs.get(i));

                if (streamI.streamType != stream0.streamType) {
                    throw new InvalidConfigurationException("Can't UNION DataStreams of different types");
                }
                if (!streamI.accessor.attributes(OBJLVL_VALUE).containsAll(stream0.accessor.attributes(OBJLVL_VALUE))
                        || !stream0.accessor.attributes(OBJLVL_VALUE).containsAll(streamI.accessor.attributes(OBJLVL_VALUE))) {
                    throw new InvalidConfigurationException("DataStreams to UNION must have same top-level record" +
                            " attributes");
                }
            }

            if (unionSpec == UnionSpec.CONCAT) {
                JavaPairRDD<Object, Record<?>>[] streams = new JavaPairRDD[inpSize];
                for (int i = 0; i < inpSize; i++) {
                    DataStream streamI = store.get(inputs.get(i));

                    streams[i] = streamI.rdd;
                }

                sourceRdd = sparkContext.<Object, Record<?>>union(streams);
            } else {
                JavaPairRDD<Tuple2<Object, Record<?>>, Integer>[] paired = new JavaPairRDD[inpSize];
                for (int i = 0; i < inpSize; i++) {
                    DataStream streamI = store.get(inputs.get(i));

                    final Integer ii = i;
                    paired[i] = streamI.rdd.mapToPair(v -> new Tuple2<>(v, ii));
                }

                JavaPairRDD<Tuple2<Object, Record<?>>, Integer> union = sparkContext.<Tuple2<Object, Record<?>>, Integer>union(paired);
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
                JavaPairRDD<Object, Record<?>> leftInputRDD = stream0.rdd;
                for (int r = 1; r < inpSize; r++) {
                    final String inputR = inputs.get(r);
                    JavaPairRDD<Object, Record<?>> rightInputRDD = store.get(inputR).rdd;

                    leftInputRDD = leftInputRDD.subtractByKey(rightInputRDD);
                }

                sourceRdd = leftInputRDD;
            } else if (joinSpec == JoinSpec.RIGHT_ANTI) {
                JavaPairRDD<Object, Record<?>> rightInputRDD = streamZ.rdd;
                for (int l = inpSize - 2; l >= 0; l--) {
                    final String inputL = inputs.get(l);
                    JavaPairRDD<Object, Record<?>> leftInputRDD = store.get(inputL).rdd;

                    rightInputRDD = rightInputRDD.subtractByKey(leftInputRDD);
                }

                resultType = streamZ.streamType;
                resultAccessor = streamZ.accessor;

                sourceRdd = rightInputRDD;
            } else if (joinSpec == JoinSpec.RIGHT) {
                resultType = streamZ.streamType;

                final Record<?> template = resultType.itemTemplate();

                Map<String, List<String>> attrs = new HashMap<>();
                attrs.put(OBJLVL_VALUE, stream0.accessor.attributes(OBJLVL_VALUE).stream()
                        .map(e -> input0 + "." + e)
                        .collect(Collectors.toList()));

                final StreamType _resultType = resultType;
                JavaPairRDD<Object, Record<?>> leftInputRDD = stream0.rdd;
                for (int l = 0, r = 1; r < inpSize; l++, r++) {
                    final String inputR = inputs.get(r);
                    final String inputL = inputs.get(l);

                    DataStream streamR = store.get(inputR);
                    attrs.get(OBJLVL_VALUE).addAll(streamR.accessor.attributes(OBJLVL_VALUE).stream()
                            .map(e -> inputR + "." + e)
                            .collect(Collectors.toList()));

                    final boolean first = (l == 0);
                    leftInputRDD = leftInputRDD.rightOuterJoin(streamR.rdd)
                            .mapPartitionsToPair(it -> {
                                List<Tuple2<Object, Record<?>>> res = new ArrayList<>();

                                while (it.hasNext()) {
                                    Tuple2<Object, Tuple2<Optional<Record<?>>, Record<?>>> o = it.next();

                                    Record<?> right = o._2._2;

                                    Record<?> merged = (Record<?>) template.clone();
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
                                    Record<?> left = o._2._1.orNull();
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

                resultAccessor = resultType.accessor(attrs);

                sourceRdd = leftInputRDD;
            } else if ((joinSpec == JoinSpec.LEFT) || (joinSpec == JoinSpec.INNER)) {
                final Record<?> template = resultType.itemTemplate();

                Map<String, List<String>> attrs = new HashMap<>();
                attrs.put(OBJLVL_VALUE, stream0.accessor.attributes(OBJLVL_VALUE).stream()
                        .map(e -> input0 + "." + e)
                        .collect(Collectors.toList()));

                final StreamType _resultType = resultType;
                JavaPairRDD<Object, Record<?>> leftInputRDD = stream0.rdd;
                for (int l = 0, r = 1; r < inpSize; l++, r++) {
                    final String inputR = inputs.get(r);
                    final String inputL = inputs.get(l);

                    DataStream streamR = store.get(inputR);
                    attrs.get(OBJLVL_VALUE).addAll(streamR.accessor.attributes(OBJLVL_VALUE).stream()
                            .map(e -> inputR + "." + e)
                            .collect(Collectors.toList()));

                    JavaPairRDD<Object, ?> partialJoin = (joinSpec == JoinSpec.LEFT)
                            ? leftInputRDD.leftOuterJoin(streamR.rdd)
                            : leftInputRDD.join(streamR.rdd);

                    final boolean first = (l == 0);
                    leftInputRDD = partialJoin
                            .mapPartitionsToPair(it -> {
                                List<Tuple2<Object, Record<?>>> res = new ArrayList<>();

                                while (it.hasNext()) {
                                    Tuple2<Object, ?> o = it.next();

                                    Tuple2<Record<?>, Object> v = (Tuple2<Record<?>, Object>) o._2;
                                    Record<?> left = v._1;

                                    Record<?> merged = null;
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
                                            merged = (Record<?>) template.clone();
                                        }
                                    }

                                    if (first) {
                                        for (Map.Entry<String, Object> e : left.asIs().entrySet()) {
                                            merged.put(inputL + "." + e.getKey(), e.getValue());
                                        }
                                    } else {
                                        merged.put(left.asIs());
                                    }

                                    Record<?> right = (v._2 instanceof Optional) ? (Record<?>) ((Optional<?>) v._2).orNull() : (Record<?>) v._2;
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

                resultAccessor = resultType.accessor(attrs);

                sourceRdd = leftInputRDD;
            } else { // OUTER
                final Record<?> template = resultType.itemTemplate();

                Map<String, List<String>> attrs = new HashMap<>();
                attrs.put(OBJLVL_VALUE, stream0.accessor.attributes(OBJLVL_VALUE).stream()
                        .map(e -> input0 + "." + e)
                        .collect(Collectors.toList()));

                final StreamType _resultType = resultType;
                JavaPairRDD<Object, Record<?>> leftInputRDD = stream0.rdd;
                for (int l = 0, r = 1; r < inpSize; l++, r++) {
                    final String inputR = inputs.get(r);
                    final String inputL = inputs.get(l);

                    DataStream streamR = store.get(inputR);
                    attrs.get(OBJLVL_VALUE).addAll(streamR.accessor.attributes(OBJLVL_VALUE).stream()
                            .map(e -> inputR + "." + e)
                            .collect(Collectors.toList()));

                    final boolean first = (l == 0);
                    leftInputRDD = leftInputRDD.fullOuterJoin(streamR.rdd)
                            .mapPartitionsToPair(it -> {
                                List<Tuple2<Object, Record<?>>> res = new ArrayList<>();

                                while (it.hasNext()) {
                                    Tuple2<Object, Tuple2<Optional<Record<?>>, Optional<Record<?>>>> o = it.next();

                                    Record<?> left = o._2._1.orNull();
                                    Record<?> right = o._2._2.orNull();

                                    Record<?> merged = (Record<?>) template.clone();
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

                resultAccessor = resultType.accessor(attrs);

                sourceRdd = leftInputRDD;
            }
        }

        final List<SelectItem> _what = items;
        final WhereItem _where = whereItem;
        final Broadcast<VariablesContext> _vc = sparkContext.broadcast(variables);
        final Accessor<? extends Record<?>> _resultAccessor = resultAccessor;
        final StreamType _resultType = resultType;
        final Record<?> _template = resultType.itemTemplate();

        JavaPairRDD<Object, Record<?>> output;

        final int size = _what.size();
        final List<String> _columns = _what.stream().map(si -> si.alias).collect(Collectors.toList());

        switch (resultType) {
            case Columnar:
            case Structured:
            case Point:
            case Polygon: {
                output = sourceRdd.mapPartitionsToPair(it -> {
                    VariablesContext vc = _vc.getValue();
                    List<Tuple2<Object, Record<?>>> ret = new ArrayList<>();

                    while (it.hasNext()) {
                        Tuple2<Object, Record<?>> rec = it.next();

                        AttrGetter getter = _resultAccessor.getter(rec._2);
                        if (Operator.bool(getter, _where.expression, vc)) {
                            if (star) {
                                ret.add(rec);
                            } else {
                                Record<?> res;
                                if (_resultType == StreamType.Point) {
                                    res = new PointEx((Geometry) rec._2);
                                } else if (_resultType == StreamType.Polygon) {
                                    res = new PolygonEx((PolygonEx) rec._2);
                                } else {
                                    res = (Record<?>) _template.clone();
                                }

                                for (int i = 0; i < size; i++) {
                                    res.put(_columns.get(i), Operator.eval(getter, _what.get(i).expression, vc));
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
                boolean queryTrack = false, querySegment = false, queryPoint = false;
                if (OBJLVL_TRACK.equals(whereItem.category)) {
                    queryTrack = true;
                }
                if (OBJLVL_SEGMENT.equals(whereItem.category)) {
                    querySegment = true;
                }
                if (OBJLVL_POINT.equals(whereItem.category)) {
                    queryPoint = true;
                }
                final boolean _qTrack = queryTrack, _qSegment = querySegment, _qPoint = queryPoint;

                output = sourceRdd.mapPartitionsToPair(it -> {
                    VariablesContext vc = _vc.getValue();
                    List<Tuple2<Object, Record<?>>> ret = new ArrayList<>();

                    while (it.hasNext()) {
                        Tuple2<Object, Record<?>> next = it.next();

                        SegmentedTrack st = (SegmentedTrack) next._2;
                        Map<String, Object> trackProps = new HashMap<>();

                        if (_qTrack) {
                            AttrGetter trackPropGetter = _resultAccessor.getter(st);
                            if (Operator.bool(trackPropGetter, _where.expression, vc)) {
                                if (star) {
                                    ret.add(next);

                                    continue;
                                } else {
                                    for (int i = 0; i < size; i++) {
                                        SelectItem selectItem = _what.get(i);

                                        if (OBJLVL_TRACK.equals(selectItem.category)) {
                                            trackProps.put(_columns.get(i), Operator.eval(trackPropGetter, selectItem.expression, vc));
                                        }
                                    }
                                    if (trackProps.isEmpty()) {
                                        trackProps = st.asIs();
                                    }
                                }
                            } else {
                                continue;
                            }
                        }

                        Geometry[] segments = st.geometries();
                        if (_qSegment) {
                            List<Geometry> segList = new ArrayList<>();

                            for (Geometry g : st) {
                                AttrGetter segPropGetter = _resultAccessor.getter((TrackSegment) g);
                                if (Operator.bool(segPropGetter, _where.expression, vc)) {
                                    segList.add(g);
                                }
                            }
                            segments = segList.toArray(new Geometry[0]);
                        }

                        for (int j = segments.length - 1; j >= 0; j--) {
                            TrackSegment g = (TrackSegment) segments[j];

                            Map<String, Object> segProps = new HashMap<>();
                            AttrGetter segPropGetter = _resultAccessor.getter(g);
                            for (int i = 0; i < size; i++) {
                                SelectItem selectItem = _what.get(i);

                                if (OBJLVL_SEGMENT.equals(selectItem.category)) {
                                    segProps.put(_columns.get(i), Operator.eval(segPropGetter, selectItem.expression, vc));
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
                                    AttrGetter pointPropGetter = _resultAccessor.getter((PointEx) gg);
                                    if (Operator.bool(pointPropGetter, _where.expression, vc)) {
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

                                AttrGetter pointPropGetter = _resultAccessor.getter(gg);
                                Map<String, Object> pointProps = new HashMap<>();
                                for (int i = 0; i < size; i++) {
                                    SelectItem selectItem = _what.get(i);

                                    if (OBJLVL_POINT.equals(selectItem.category)) {
                                        pointProps.put(_columns.get(i), Operator.eval(pointPropGetter, selectItem.expression, vc));
                                    }
                                }

                                if (!pointProps.isEmpty()) {
                                    PointEx point = new PointEx(gg);
                                    point.put(pointProps);

                                    points[j] = point;
                                }
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

        if (distinct) {
            output = output.distinct();
        }

        if (limitRecords != null) {
            output = output.sample(false, limitRecords.doubleValue() / output.count());
        }
        if (limitPercent != null) {
            output = output.sample(false, limitPercent);
        }

        return output;
    }

    public Collection<Object> subQuery(boolean distinct, DataStream input, List<Expression<?>> item, List<Expression<?>> query, Double limitPercent, Long limitRecords, VariablesContext variables) {
        final Accessor<? extends Record<?>> acc = input.accessor;

        final List<Expression<?>> _what = item;
        final List<Expression<?>> _query = query;
        final Broadcast<VariablesContext> _vc = sparkContext.broadcast(variables);

        JavaRDD<Object> output = input.rdd
                .mapPartitions(it -> {
                    VariablesContext vc = _vc.getValue();
                    List<Object> ret = new ArrayList<>();

                    while (it.hasNext()) {
                        Record<?> rec = it.next()._2;

                        AttrGetter getter = acc.getter(rec);
                        if (Operator.bool(getter, _query, vc)) {
                            ret.add(Operator.eval(getter, _what, vc));
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

    public void analyze(String dsName, String counterColumn) {
        JavaPairRDD<Object, Record<?>> rdd = get(METRICS_DS).rdd;
        List<Tuple2<Object, Record<?>>> metricsList = new ArrayList<>(rdd.collect());

        for (Map.Entry<String, DataStream> e : getAll(dsName).entrySet()) {
            String streamName = e.getKey();
            DataStream ds = e.getValue();

            List<String> columns = ds.accessor.attributes(OBJLVL_VALUE);

            final String _counterColumn = columns.contains(counterColumn) ? counterColumn : null;

            JavaPairRDD<Object, Object> rdd2 = ds.rdd.mapPartitionsToPair(it -> {
                List<Tuple2<Object, Object>> ret = new ArrayList<>();
                while (it.hasNext()) {
                    Tuple2<Object, Record<?>> r = it.next();

                    Object id;
                    if (_counterColumn == null) {
                        id = r._1;
                    } else {
                        id = r._2.asIs(_counterColumn);
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

            int uniqueCounters = counts.size();
            long totalCount = counts.stream().reduce(Long::sum).orElse(0L);
            double counterAverage = (uniqueCounters == 0) ? 0.D : ((double) totalCount / uniqueCounters);
            double counterMedian = 0.D;
            if (uniqueCounters != 0) {
                int m = (uniqueCounters <= 2) ? 0 : (uniqueCounters >> 1);
                counterMedian = ((uniqueCounters % 2) == 0) ? (counts.get(m) + counts.get(m + 1)) / 2.D : counts.get(m).doubleValue();
            }

            Columnar rec = new Columnar(METRICS_COLUMNS, new Object[]{streamName, ds.streamType.name(), ds.rdd.getNumPartitions(),
                    counterColumn, totalCount, uniqueCounters, counterAverage, counterMedian});
            metricsList.add(new Tuple2<>(streamName, rec));
        }

        put(Constants.METRICS_DS, new DataStream(StreamType.Columnar,
                sparkContext.parallelizePairs(metricsList, 1),
                Collections.singletonMap(OBJLVL_VALUE, METRICS_COLUMNS)));
    }
}
