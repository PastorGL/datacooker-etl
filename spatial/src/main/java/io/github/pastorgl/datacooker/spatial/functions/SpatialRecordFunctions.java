/**
 * Copyright (C) 2024 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.spatial.functions;

import io.github.pastorgl.datacooker.data.spatial.PolygonEx;
import io.github.pastorgl.datacooker.data.spatial.SegmentedTrack;
import io.github.pastorgl.datacooker.data.spatial.TrackSegment;
import io.github.pastorgl.datacooker.scripting.Function.RecordLevel;
import net.sf.geographiclib.Geodesic;
import net.sf.geographiclib.PolygonArea;
import net.sf.geographiclib.PolygonResult;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;

import java.util.Deque;

@SuppressWarnings("unused")
public class SpatialRecordFunctions {
    public static class PolyArea extends RecordLevel<Double, PolygonEx> {
        @Override
        public Double call(Deque<Object> args) {
            PolygonEx poly = (PolygonEx) args.pop();

            PolygonArea pArea = new PolygonArea(Geodesic.WGS84, false);

            for (Coordinate c : poly.getExteriorRing().getCoordinates()) {
                pArea.AddPoint(c.y, c.x);
            }

            PolygonResult pRes = pArea.Compute();

            double area = Math.abs(pRes.area);
            for (int hole = poly.getNumInteriorRing(); hole > 0; hole--) {
                LineString lr = poly.getInteriorRingN(hole - 1);

                pArea.Clear();
                for (Coordinate c : lr.getCoordinates()) {
                    pArea.AddPoint(c.y, c.x);
                }

                area -= Math.abs(pArea.Compute().area);
            }

            return area;
        }

        @Override
        public String name() {
            return "POLY_AREA";
        }

        @Override
        public String descr() {
            return "The area of the Polygon in square meters";
        }
    }

    public static class PolyHoles extends RecordLevel<Integer, PolygonEx> {
        @Override
        public Integer call(Deque<Object> args) {
            PolygonEx poly = (PolygonEx) args.pop();

            return poly.getNumInteriorRing();
        }

        @Override
        public String name() {
            return "POLY_HOLES";
        }

        @Override
        public String descr() {
            return "Number of holes in the Polygon";
        }
    }

    public static class PolyVertices extends RecordLevel<Integer, PolygonEx> {
        @Override
        public Integer call(Deque<Object> args) {
            PolygonEx poly = (PolygonEx) args.pop();

            return poly.getExteriorRing().getNumGeometries() - 1;
        }

        @Override
        public String name() {
            return "POLY_VERTICES";
        }

        @Override
        public String descr() {
            return "Number of Polygon's outline vertices";
        }
    }

    public static class PolyPerimeter extends RecordLevel<Double, PolygonEx> {
        @Override
        public Double call(Deque<Object> args) {
            PolygonEx poly = (PolygonEx) args.pop();

            PolygonArea pArea = new PolygonArea(Geodesic.WGS84, false);

            for (Coordinate c : poly.getExteriorRing().getCoordinates()) {
                pArea.AddPoint(c.y, c.x);
            }

            PolygonResult pRes = pArea.Compute();
            return pRes.perimeter;
        }

        @Override
        public String name() {
            return "POLY_PERIMETER";
        }

        @Override
        public String descr() {
            return "The perimeter of the Polygon's outline in meters";
        }
    }

    public static class TrackPoints extends RecordLevel<Integer, SegmentedTrack> {
        @Override
        public Integer call(Deque<Object> args) {
            SegmentedTrack track = (SegmentedTrack) args.pop();

            int ret = 0;
            for (Geometry g : track.geometries()) {
                for (Geometry gg : ((TrackSegment) g).geometries()) {
                    ret += gg.getNumGeometries();
                }
            }

            return ret;
        }

        @Override
        public String name() {
            return "TRK_POINTS";
        }

        @Override
        public String descr() {
            return "Number of Points in the Track";
        }
    }

    public static class TrackSegments extends RecordLevel<Integer, PolygonEx> {
        @Override
        public Integer call(Deque<Object> args) {
            SegmentedTrack track = (SegmentedTrack) args.pop();

            return track.getNumGeometries();
        }

        @Override
        public String name() {
            return "TRK_SEGMENTS";
        }

        @Override
        public String descr() {
            return "Number of Segments in the Track";
        }
    }
}
