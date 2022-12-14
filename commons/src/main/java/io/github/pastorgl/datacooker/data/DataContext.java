/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.data;

import io.github.pastorgl.datacooker.config.Constants;
import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.data.spatial.PointEx;
import io.github.pastorgl.datacooker.data.spatial.PolygonEx;
import io.github.pastorgl.datacooker.data.spatial.SegmentedTrack;
import io.github.pastorgl.datacooker.data.spatial.TrackSegment;
import io.github.pastorgl.datacooker.scripting.*;
import org.apache.commons.collections4.map.ListOrderedMap;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.*;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import scala.Tuple2;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.github.pastorgl.datacooker.config.Constants.*;

@SuppressWarnings("unchecked")
public class DataContext {
    static public final List<String> METRICS_COLUMNS = Arrays.asList("_streamName", "_streamType", "_counterColumn", "_totalCount", "_uniqueCounters", "_counterAverage", "_counterMedian");

    private final JavaSparkContext sparkContext;
    public final RDDUtils utils;

    private StorageLevel sl = StorageLevel.MEMORY_AND_DISK();
    private int ut = 2;

    private final HashMap<String, DataStream> store = new HashMap<>();

    private VariablesContext options;

    public DataContext(final JavaSparkContext sparkContext) {
        this.sparkContext = sparkContext;

        this.utils = new RDDUtils() {
            @Override
            public <T> Broadcast<T> broadcast(T broadcast) {
                return sparkContext.broadcast(broadcast);
            }

            @Override
            public <T> JavaRDD<T> union(JavaRDD... rddArray) {
                return sparkContext.<T>union(rddArray);
            }

            @Override
            public <K, V> JavaPairRDD<K, V> union(JavaPairRDD... rddArray) {
                return sparkContext.<K, V>union(rddArray);
            }

            @Override
            public <T> JavaRDD<T> parallelize(List<T> list, int partCount) {
                return sparkContext.parallelize(list, partCount);
            }

            @Override
            public <K, V> JavaPairRDD<K, V> parallelizePairs(List<Tuple2<K, V>> list, int partCount) {
                return sparkContext.parallelizePairs(list, partCount);
            }

            @Override
            public <T> JavaRDD<T> empty() {
                return sparkContext.emptyRDD();
            }
        };

        store.put(Constants.METRICS_DS, new DataStream(StreamType.Columnar, sparkContext.emptyRDD(), Collections.singletonMap(OBJLVL_VALUE, METRICS_COLUMNS)));
    }

    public void initialize(VariablesContext options) {
        this.options = options;

        String storageLevel = options.getString("storage.level");
        if (storageLevel != null) {
            sl = StorageLevel.fromString(storageLevel);
        }
        Number usageThreshold = options.getNumber("usage.threshold");
        if (usageThreshold != null) {
            ut = usageThreshold.intValue();
        }
    }

    public DataStream get(String dsName) {
        if (store.containsKey(dsName)) {
            return store.get(dsName);
        }

        throw new InvalidConfigurationException("Reference to undefined DataStream '" + dsName + "'");
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
                dataStream.underlyingRdd.rdd().persist(sl);
            }

            ret.put(name, dataStream);
        }

        return ret;
    }

    public void put(String name, DataStream ds) {
        if (store.containsKey(name)) {
            throw new InvalidConfigurationException("Can't CREATE DS \"" + name + "\", because it is already defined");
        }

        store.put(name, ds);
    }

    public Map<String, DataStream> result() {
        return store;
    }

    public void createDataStream(String inputName, Map<String, Object> params) {
        if (!params.containsKey("path")) {
            throw new InvalidConfigurationException("CREATE DS \"" + inputName + "\" statement must have @path parameter, but it doesn't");
        }

        String path = sparkContext.isLocal() ? inputPathLocal(inputName, (String) params.get("path")) : inputPath(inputName, (String) params.get("path"));

        int parts = params.containsKey("part_count") ? ((Number) params.get("part_count")).intValue() : 1;
        JavaRDDLike inputRdd = sparkContext.textFile(path, Math.max(parts, 1));

        inputRdd.rdd().setName("datacooker:input:" + inputName);
        put(inputName, new DataStream(inputRdd));
    }

    public void copyDataStream(String outputName, boolean star, Map<String, Object> params) {
        if (star) {
            ListOrderedMap<String, DataStream> dataStreams = getAll(outputName);

            for (Map.Entry<String, DataStream> dse : dataStreams.entrySet()) {
                DataStream ds = dse.getValue();

                save(outputName, params, ds);
            }
        } else {
            if (store.containsKey(outputName)) {
                save(outputName, params, store.get(outputName));
            } else {
                throw new InvalidConfigurationException("COPY DS \"" + outputName + "\" refers to nonexistent DataStream");
            }
        }
    }

    private void save(String outputName, Map<String, Object> params, DataStream ds) {
        JavaRDDLike rdd = ds.get();
        rdd.rdd().setName("datacooker:output:" + outputName);

        String path = sparkContext.isLocal() ? outputPathLocal(outputName, (String) params.get("path")) : outputPath(outputName, (String) params.get("path"));

        rdd.saveAsTextFile(path);
    }

    public void replaceData(String name, JavaRDDLike rdd) {
        DataStream ds = store.get(name);
        if (ds != null) {
            ds.underlyingRdd = rdd;
        }
    }

    public void alterDataStream(String dsName, StreamConverter converter, StreamType reqType, Map<String, List<String>> newColumns, List<Expression<?>> keyExpression, ParamsContext params) {
        DataStream ds = store.get(dsName);

        if (reqType == StreamType.KeyValue) {
            if ((keyExpression != null) && (ds.streamType != StreamType.PlainText)) {
                final Accessor acc = ds.accessor;
                ds.underlyingRdd = ((JavaRDD<Object>) ds.underlyingRdd)
                        .mapPartitionsToPair(it -> {
                            List<Tuple2<String, Record>> ret = new ArrayList<>();

                            while (it.hasNext()) {
                                Record rec = (Record) it.next();
                                AttrGetter getter = acc.getter(rec);

                                ret.add(new Tuple2<>(String.valueOf(Operator.eval(getter, keyExpression, null)), rec));
                            }

                            return ret.iterator();
                        });
            } else {
                ds.underlyingRdd = ((JavaRDD<Object>) ds.underlyingRdd)
                        .mapToPair(t -> new Tuple2<>(t.hashCode(), t));
            }
        }
        ds = converter.apply(ds, newColumns, params);

        store.replace(dsName, ds);
    }

    public RDDUtils getUtils() {
        return utils;
    }

    public boolean has(String dsName) {
        return store.containsKey(dsName);
    }

    public String inputPathLocal(String name, String path) {
        return options.getString(Constants.PATH) + "/" + name + "/*";
    }

    public String inputPath(String name, String path) {
        if (path == null) {
            return options.getString("input.path") + "/" + name;
        }

        return path;
    }

    public String outputPath(String name, String path) {
        return options.getString(Constants.PATH) + "/" + name;
    }

    public String outputPathLocal(String name, String path) {
        if (path == null) {
            return options.getString("output.path") + "/" + name;
        }

        return path;
    }

    public JavaRDDLike select(boolean distinct, List<String> inputs, UnionSpec unionSpec, JoinSpec joinSpec, final boolean star, List<SelectItem> items, QueryItem query, Double limitPercent, Long limitRecords, VariablesContext variables) {
        final int inpNumber = inputs.size();

        String input0 = inputs.get(0);
        DataStream stream0 = store.get(input0);
        StreamType resultType = stream0.streamType;

        if ((unionSpec != null) || (joinSpec != null)) {
            if (inputs.size() < 2) {
                throw new InvalidConfigurationException("SELECT UNION or JOIN requires multiple DataStreams");
            }
        }

        JavaRDDLike sourceRdd;
        Accessor resultAccessor;

        if (unionSpec != null) {
            resultAccessor = stream0.accessor;

            List<JavaPairRDD<Object, Integer>> paired = new ArrayList<>();
            paired.add(stream0.underlyingRdd.mapToPair(v -> new Tuple2<>(v, 0)));
            for (int i = 1; i < inpNumber; i++) {
                DataStream streamI = store.get(inputs.get(i));

                if (streamI.streamType != resultType) {
                    throw new InvalidConfigurationException("Can't UNION DataStreams of different types");
                }
                if (!streamI.accessor.attributes(OBJLVL_VALUE).containsAll(resultAccessor.attributes(OBJLVL_VALUE))
                        || !resultAccessor.attributes(OBJLVL_VALUE).containsAll(streamI.accessor.attributes(OBJLVL_VALUE))) {
                    throw new InvalidConfigurationException("UNION-ized DataStreams must have same top-level record attributes");
                }

                final Integer ii = i;
                paired.add(streamI.underlyingRdd.mapToPair(v -> new Tuple2<>(v, ii)));
            }

            JavaPairRDD<Object, Integer> union = sparkContext.<Object, Integer>union(paired.toArray(new JavaPairRDD[0]));
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
                            .flatMap(t -> Stream.generate(() -> t._1).limit(t._2).iterator());
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
                                if (inpSet.size() < inpNumber) {
                                    return 0L;
                                } else {
                                    return counts.values().stream().mapToLong(Long::longValue).reduce(Math::min).orElse(0L);
                                }
                            })
                            .flatMap(t -> Stream.generate(() -> t._1).limit(t._2).iterator());
                    break;
                }
                default: {
                    sourceRdd = union
                            .keys();
                    break;
                }
            }
        } else if (joinSpec != null) {
            for (String input : inputs) {
                DataStream streamI = store.get(input);

                if (streamI.streamType != StreamType.KeyValue) {
                    throw new InvalidConfigurationException("Can't JOIN non-KeyValue DataStreams");
                }
            }

            resultAccessor = StreamType.KeyValue.accessor(Collections.singletonMap(OBJLVL_VALUE, stream0.accessor.attributes(OBJLVL_VALUE).stream()
                    .map(e -> input0 + "." + e).collect(Collectors.toList())));

            JavaPairRDD leftInputRDD = ((JavaPairRDD) stream0.underlyingRdd);
            for (int r = 1; r < inputs.size(); r++) {
                final String inputR = inputs.get(r);
                JavaPairRDD rightInputRDD = (JavaPairRDD) store.get(inputR).underlyingRdd;

                JavaPairRDD partialJoin = null;
                switch (joinSpec) {
                    case LEFT: {
                        partialJoin = leftInputRDD.leftOuterJoin(rightInputRDD);
                        break;
                    }
                    case RIGHT: {
                        partialJoin = leftInputRDD.rightOuterJoin(rightInputRDD);
                        break;
                    }
                    case OUTER: {
                        partialJoin = leftInputRDD.fullOuterJoin(rightInputRDD);
                        break;
                    }
                    case LEFT_ANTI: {
                        leftInputRDD = leftInputRDD.subtractByKey(rightInputRDD);
                        break;
                    }
                    case RIGHT_ANTI: {
                        leftInputRDD = rightInputRDD.subtractByKey(leftInputRDD);
                        break;
                    }
                    default: { //INNER
                        partialJoin = leftInputRDD.join(rightInputRDD);
                    }
                }

                if (joinSpec == JoinSpec.RIGHT_ANTI) {
                    resultAccessor = StreamType.KeyValue.accessor(Collections.singletonMap(OBJLVL_VALUE, store.get(inputR).accessor.attributes(OBJLVL_VALUE).stream()
                            .map(e -> inputR + "." + e).collect(Collectors.toList())));
                } else if (joinSpec != JoinSpec.LEFT_ANTI) {
                    final String inputL = inputs.get(r - 1);
                    leftInputRDD = partialJoin.mapPartitionsToPair(ito -> {
                        List<Tuple2> res = new ArrayList<>();

                        Iterator<Tuple2<Object, Object>> it = (Iterator) ito;

                        while (it.hasNext()) {
                            Tuple2<Object, Object> o = it.next();

                            Tuple2<Object, Object> v = (Tuple2<Object, Object>) o._2;

                            Columnar left = null;
                            if (v._1 instanceof Optional) {
                                Optional o1 = (Optional) v._1;
                                if (o1.isPresent()) {
                                    left = (Columnar) o1.get();
                                }
                            } else {
                                left = (Columnar) v._1;
                            }

                            Columnar right = null;
                            if (v._2 instanceof Optional) {
                                Optional o2 = (Optional) v._2;
                                if (o2.isPresent()) {
                                    right = (Columnar) o2.get();
                                }
                            } else {
                                right = (Columnar) v._2;
                            }

                            Columnar merge = new Columnar();
                            if (left != null) {
                                merge.put(left.asIs().entrySet().stream()
                                        .collect(Collectors.toMap(e -> inputL + "." + e.getKey(), Map.Entry::getValue, (a, b) -> a, ListOrderedMap::new)));
                            }
                            if (right != null) {
                                merge.put(right.asIs().entrySet().stream()
                                        .collect(Collectors.toMap(e -> inputR + "." + e.getKey(), Map.Entry::getValue, (a, b) -> a, ListOrderedMap::new)));
                            }
                            res.add(new Tuple2<>(o._1, merge));
                        }

                        return res.iterator();
                    });

                    Stream<String> concat = Stream.concat(
                            resultAccessor.attributes(OBJLVL_VALUE).stream(),
                            store.get(inputR).accessor.attributes(OBJLVL_VALUE).stream()
                                    .map(e -> inputR + "." + e)
                    );
                    resultAccessor = StreamType.KeyValue.accessor(Collections.singletonMap(OBJLVL_VALUE, concat
                            .collect(Collectors.toList())));
                }
            }

            sourceRdd = leftInputRDD;
        } else {
            sourceRdd = stream0.underlyingRdd;
            resultAccessor = stream0.accessor;
        }

        final List<SelectItem> _what = items;
        final QueryItem _query = query;
        final Broadcast<VariablesContext> _vc = sparkContext.broadcast(variables);
        final Accessor _acc = resultAccessor;

        JavaRDDLike output;

        final int size = _what.size();
        final List<String> _columns = _what.stream().map(si -> si.alias).collect(Collectors.toList());
        if (sourceRdd instanceof JavaRDD) {
            switch (resultType) {
                case Columnar: {
                    output = ((JavaRDD<Columnar>) sourceRdd)
                            .mapPartitions(it -> {
                                VariablesContext vc = _vc.getValue();
                                List<Columnar> ret = new ArrayList<>();

                                while (it.hasNext()) {
                                    Columnar rec = it.next();

                                    AttrGetter getter = _acc.getter(rec);
                                    if (Operator.bool(getter, _query.expression, vc)) {
                                        Columnar res = new Columnar(_columns);
                                        if (star) {
                                            res.put(rec.asIs());
                                        } else {
                                            for (int i = 0; i < size; i++) {
                                                res.put(_columns.get(i), Operator.eval(getter, _what.get(i).expression, vc));
                                            }
                                        }

                                        ret.add(res);
                                    }
                                }

                                return ret.iterator();
                            });
                    break;
                }
                case Point: {
                    output = ((JavaRDD<PointEx>) sourceRdd)
                            .mapPartitions(it -> {
                                VariablesContext vc = _vc.getValue();
                                List<PointEx> ret = new ArrayList<>();

                                while (it.hasNext()) {
                                    PointEx p = it.next();

                                    AttrGetter propGetter = _acc.getter(p);
                                    if (Operator.bool(propGetter, _query.expression, vc)) {
                                        PointEx res = new PointEx(p);
                                        if (star) {
                                            res.put((Map) p.getUserData());
                                        } else {
                                            for (int i = 0; i < size; i++) {
                                                res.put(_columns.get(i), Operator.eval(propGetter, _what.get(i).expression, vc));
                                            }
                                        }

                                        ret.add(res);
                                    }
                                }

                                return ret.iterator();
                            });
                    break;
                }
                case Track: {
                    boolean queryTrack = false, querySegment = false, queryPoint = false;
                    if (OBJLVL_TRACK.equals(query.category)) {
                        queryTrack = true;
                    }
                    if (OBJLVL_SEGMENT.equals(query.category)) {
                        querySegment = true;
                    }
                    if (OBJLVL_POINT.equals(query.category)) {
                        queryPoint = true;
                    }
                    final boolean _qTrack = queryTrack, _qSegment = querySegment, _qPoint = queryPoint;

                    output = ((JavaRDD<SegmentedTrack>) sourceRdd)
                            .mapPartitions(it -> {
                                VariablesContext vc = _vc.getValue();
                                List<SegmentedTrack> ret = new ArrayList<>();

                                while (it.hasNext()) {
                                    SegmentedTrack st = it.next();
                                    Map<String, Object> trackProps = new HashMap<>();

                                    if (_qTrack) {
                                        AttrGetter trackPropGetter = _acc.getter(st);
                                        if (Operator.bool(trackPropGetter, _query.expression, vc)) {
                                            if (star) {
                                                ret.add(st);

                                                continue;
                                            } else {
                                                for (int i = 0; i < size; i++) {
                                                    SelectItem selectItem = _what.get(i);

                                                    if (OBJLVL_TRACK.equals(selectItem.category)) {
                                                        trackProps.put(_columns.get(i), Operator.eval(trackPropGetter, selectItem.expression, vc));
                                                    }
                                                }
                                                if (trackProps.isEmpty()) {
                                                    trackProps = (Map) st.getUserData();
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
                                            AttrGetter segPropGetter = _acc.getter(g);
                                            if (Operator.bool(segPropGetter, _query.expression, vc)) {
                                                segList.add(g);
                                            }
                                        }
                                        segments = segList.toArray(new Geometry[0]);
                                    }

                                    GeometryFactory geometryFactory = st.getFactory();

                                    for (int j = segments.length - 1; j >= 0; j--) {
                                        Geometry g = segments[j];

                                        Map<String, Object> segProps = new HashMap<>();
                                        AttrGetter segPropGetter = _acc.getter(g);
                                        for (int i = 0; i < size; i++) {
                                            SelectItem selectItem = _what.get(i);

                                            if (OBJLVL_SEGMENT.equals(selectItem.category)) {
                                                segProps.put(_columns.get(i), Operator.eval(segPropGetter, selectItem.expression, vc));
                                            }
                                        }

                                        if (segProps.isEmpty()) {
                                            segProps = (Map) g.getUserData();
                                        }

                                        TrackSegment seg = new TrackSegment(((TrackSegment) g).geometries(), geometryFactory);
                                        seg.setUserData(segProps);
                                        segments[j] = seg;
                                    }

                                    if (_qPoint) {
                                        List<Geometry> pSegs = new ArrayList<>();
                                        for (Geometry g : segments) {
                                            TrackSegment seg = (TrackSegment) g;

                                            List<Geometry> points = new ArrayList<>();
                                            for (Geometry gg : seg) {
                                                AttrGetter pointPropGetter = _acc.getter(gg);
                                                if (Operator.bool(pointPropGetter, _query.expression, vc)) {
                                                    points.add(gg);
                                                }
                                            }

                                            if (!points.isEmpty()) {
                                                TrackSegment pSeg = new TrackSegment(points.toArray(new Geometry[0]), geometryFactory);
                                                pSeg.setUserData(pSeg.getUserData());
                                                pSegs.add(pSeg);
                                            }
                                        }

                                        segments = pSegs.toArray(new Geometry[0]);
                                    }

                                    for (int k = segments.length - 1; k >= 0; k--) {
                                        TrackSegment g = (TrackSegment) segments[k];
                                        Map<String, Object> segProps = (Map) g.getUserData();

                                        Geometry[] points = g.geometries();
                                        for (int j = points.length - 1; j >= 0; j--) {
                                            PointEx gg = (PointEx) points[j];

                                            AttrGetter pointPropGetter = _acc.getter(gg);
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

                                        TrackSegment seg = new TrackSegment(points, geometryFactory);
                                        seg.setUserData(segProps);
                                        segments[k] = seg;
                                    }

                                    if (segments.length > 0) {
                                        SegmentedTrack rst = new SegmentedTrack(segments, geometryFactory);
                                        rst.setUserData(trackProps);
                                        ret.add(rst);
                                    }
                                }

                                return ret.iterator();
                            });
                    break;
                }
                case Polygon: {
                    output = ((JavaRDD<PolygonEx>) sourceRdd)
                            .mapPartitions(it -> {
                                VariablesContext vc = _vc.getValue();
                                List<PolygonEx> ret = new ArrayList<>();

                                while (it.hasNext()) {
                                    PolygonEx p = it.next();

                                    AttrGetter propGetter = _acc.getter(p);
                                    if (Operator.bool(propGetter, _query.expression, vc)) {
                                        PolygonEx res = new PolygonEx(p);
                                        if (star) {
                                            res.put((Map) p.getUserData());
                                        } else {
                                            for (int i = 0; i < size; i++) {
                                                res.put(_columns.get(i), Operator.eval(propGetter, _what.get(i).expression, vc));
                                            }
                                        }

                                        ret.add(res);
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
                output = ((JavaRDD) output).distinct();
            }

            if (limitRecords != null) {
                output = ((JavaRDD) output).sample(false, limitRecords.doubleValue() / output.count());
            }
            if (limitPercent != null) {
                output = ((JavaRDD) output).sample(false, limitPercent);
            }
        } else {
            output = ((JavaPairRDD<Object, Columnar>) sourceRdd)
                    .mapPartitionsToPair(it -> {
                        VariablesContext vc = _vc.getValue();
                        List<Tuple2<Object, Columnar>> ret = new ArrayList<>();

                        while (it.hasNext()) {
                            Tuple2<Object, Columnar> rec = it.next();

                            AttrGetter getter = _acc.getter(rec._2);
                            if (Operator.bool(getter, _query.expression, vc)) {
                                Columnar res = new Columnar(_columns);
                                if (star) {
                                    res.put(rec._2.asIs());
                                } else {
                                    for (int i = 0; i < size; i++) {
                                        res.put(_columns.get(i), Operator.eval(getter, _what.get(i).expression, vc));
                                    }
                                }

                                ret.add(new Tuple2<>(rec._1, res));
                            }
                        }

                        return ret.iterator();
                    });

            if (distinct) {
                output = ((JavaPairRDD) output).distinct();
            }

            if (limitRecords != null) {
                output = ((JavaPairRDD) output).sample(false, limitRecords.doubleValue() / output.count());
            }
            if (limitPercent != null) {
                output = ((JavaPairRDD) output).sample(false, limitPercent);
            }
        }

        return output;
    }

    public JavaRDDLike subQuery(boolean distinct, String input, List<Expression<?>> item, List<Expression<?>> query, Double limitPercent, Long limitRecords, VariablesContext variables) {
        JavaRDDLike srcRdd = store.get(input).underlyingRdd;

        Accessor acc = store.get(input).accessor;

        final List<Expression<?>> _what = item;
        final List<Expression<?>> _query = query;
        final Broadcast<VariablesContext> _vc = sparkContext.broadcast(variables);
        final Accessor _acc = acc;

        JavaRDDLike output;

        if (srcRdd instanceof JavaRDD) {
            output = ((JavaRDD<Columnar>) srcRdd)
                    .mapPartitions(it -> {
                        VariablesContext vc = _vc.getValue();
                        List<Object> ret = new ArrayList<>();

                        while (it.hasNext()) {
                            Columnar rec = it.next();

                            AttrGetter getter = _acc.getter(rec);
                            if (Operator.bool(getter, _query, vc)) {
                                ret.add(Operator.eval(getter, _what, vc));
                            }
                        }

                        return ret.iterator();
                    });

            if (distinct) {
                output = ((JavaRDD) output).distinct();
            }

            if (limitRecords != null) {
                output = ((JavaRDD) output).sample(false, limitRecords.doubleValue() / output.count());
            }
            if (limitPercent != null) {
                output = ((JavaRDD) output).sample(false, limitPercent);
            }
        } else {
            output = ((JavaPairRDD<Object, Columnar>) srcRdd)
                    .mapPartitions(it -> {
                        VariablesContext vc = _vc.getValue();
                        List<Object> ret = new ArrayList<>();

                        while (it.hasNext()) {
                            Tuple2<Object, Columnar> rec = it.next();

                            AttrGetter getter = _acc.getter(rec._2);
                            if (Operator.bool(getter, _query, vc)) {

                                ret.add(Operator.eval(getter, _what, vc));
                            }
                        }

                        return ret.iterator();
                    });

            if (distinct) {
                output = ((JavaPairRDD) output).distinct();
            }

            if (limitRecords != null) {
                output = ((JavaPairRDD) output).sample(false, limitRecords.doubleValue() / output.count());
            }
            if (limitPercent != null) {
                output = ((JavaPairRDD) output).sample(false, limitPercent);
            }
        }

        return output;
    }
}
