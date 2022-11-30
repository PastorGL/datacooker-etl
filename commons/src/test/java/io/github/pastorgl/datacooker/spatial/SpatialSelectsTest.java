/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.spatial;

import io.github.pastorgl.datacooker.data.spatial.PointEx;
import io.github.pastorgl.datacooker.data.spatial.SegmentedTrack;
import io.github.pastorgl.datacooker.data.spatial.TrackSegment;
import io.github.pastorgl.datacooker.scripting.TestRunner;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class SpatialSelectsTest {
    @Test
    public void spatialSelectTest() {
        try (TestRunner underTest = new TestRunner("/test.spatialSelect.tdl")) {
            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<SegmentedTrack> rddS = (JavaRDD<SegmentedTrack>) ret.get("ret1");
            SegmentedTrack st = rddS.first();
            assertEquals(2, st.getNumGeometries());
            assertEquals(
                    10,
                    st.getGeometryN(0).getNumGeometries()
            );

            rddS = (JavaRDD<SegmentedTrack>) ret.get("ret2");
            st = rddS.first();
            List<Geometry> points = new ArrayList<>();
            for (int i = st.getNumGeometries() - 1; i >= 0; i--) {
                points.addAll(Arrays.asList(((TrackSegment) st.getGeometryN(i)).geometries()));
            }
            List<PointEx> datas = points.stream()
                    .map(t -> (PointEx) t)
                    .collect(Collectors.toList());
            assertEquals(4, datas.size());
            for (PointEx data : datas) {
                double acc = data.asDouble("acc");
                assertTrue(acc >= 15.D);
                assertTrue(acc < 100.D);
            }

            rddS = (JavaRDD<SegmentedTrack>) ret.get("ret3");
            st = rddS.first();
            points = new ArrayList<>();
            for (int i = st.getNumGeometries() - 1; i >= 0; i--) {
                points.addAll(Arrays.asList(((TrackSegment) st.getGeometryN(i)).geometries()));
            }
            datas = points.stream()
                    .map(t -> (PointEx) t)
                    .collect(Collectors.toList());
            assertEquals(15, datas.size());
            Pattern p = Pattern.compile(".+?non.*");
            for (PointEx data : datas) {
                String pt = data.asString("pt");
                String trackid = data.asString("trackid");
                assertTrue("e2e".equals(pt) || p.matcher(trackid).matches());
            }

            rddS = (JavaRDD<SegmentedTrack>) ret.get("ret4");
            st = rddS.first();
            assertEquals(13, st.getNumGeometries());

            rddS = (JavaRDD<SegmentedTrack>) ret.get("ret5");
            assertEquals(0, rddS.count());

            rddS = (JavaRDD<SegmentedTrack>) ret.get("ret6");
            st = rddS.first();
            assertEquals(11, st.getNumGeometries());

            rddS = (JavaRDD<SegmentedTrack>) ret.get("ret7");
            st = rddS.first();
            points = new ArrayList<>();
            for (int i = st.getNumGeometries() - 1; i >= 0; i--) {
                points.addAll(Arrays.asList(((TrackSegment) st.getGeometryN(i)).geometries()));
            }
            datas = points.stream()
                    .map(t -> (PointEx) t)
                    .collect(Collectors.toList());
            assertEquals(33, datas.size());
            for (PointEx data : datas) {
                double acc = data.asDouble("acc");
                assertTrue(acc < 15.D || acc >= 100.D);
            }

            rddS = (JavaRDD<SegmentedTrack>) ret.get("ret8");
            st = rddS.first();
            points = new ArrayList<>();
            for (int i = st.getNumGeometries() - 1; i >= 0; i--) {
                points.addAll(Arrays.asList(((TrackSegment) st.getGeometryN(i)).geometries()));
            }
            datas = points.stream()
                    .map(t -> (PointEx) t)
                    .collect(Collectors.toList());
            assertEquals(22, datas.size());
            for (PointEx data : datas) {
                String pt = data.asString("pt");
                String trackid = data.asString("trackid");
                assertFalse(!"e2e".equals(pt) && p.matcher(trackid).matches());
            }

            rddS = (JavaRDD<SegmentedTrack>) ret.get("ret9");
            assertEquals(0, rddS.count());

            rddS = (JavaRDD<SegmentedTrack>) ret.get("ret10");
            assertEquals(0, rddS.count());
        }
    }

    @Test
    public void selectByPropertyTest() {
        try (TestRunner underTest = new TestRunner("/test2.spatialSelect.tdl")) {
            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<PointEx> rddS = (JavaRDD<PointEx>) ret.get("ret11");
            assertEquals(9, rddS.count());

            rddS = (JavaRDD<PointEx>) ret.get("ret12");
            assertEquals(28, rddS.count());

            rddS = (JavaRDD<PointEx>) ret.get("ret13");
            assertEquals(35, rddS.count());

            rddS = (JavaRDD<PointEx>) ret.get("ret14");
            assertEquals(0, rddS.count());
        }
    }
}
