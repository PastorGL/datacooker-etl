/**
 * Copyright (C) 2025 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.spatial.functions;

import io.github.pastorgl.datacooker.data.ArrayWrap;
import io.github.pastorgl.datacooker.data.spatial.PointEx;
import io.github.pastorgl.datacooker.scripting.Evaluator;
import io.github.pastorgl.datacooker.scripting.Function;
import net.sf.geographiclib.Geodesic;
import net.sf.geographiclib.GeodesicData;
import net.sf.geographiclib.GeodesicMask;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.impl.CoordinateArraySequence;

import java.util.Deque;

@SuppressWarnings("unused")
public class PointFunctions {
    public static class PointDirect extends Function.Ternary<PointEx, PointEx, Double, Double> {
        @Override
        public PointEx call(Deque<Object> args) {
            PointEx src = (PointEx) args.pop();
            double s12 = Evaluator.popDouble(args);
            double azi = Evaluator.popDouble(args);

            GeodesicData gd = Geodesic.WGS84.Direct(src.getY(), src.getX(), azi, s12, GeodesicMask.LATITUDE | GeodesicMask.LONGITUDE);
            return new PointEx(new CoordinateArraySequence(new Coordinate[]{new CoordinateXY(gd.lon2, gd.lat2)}));
        }

        @Override
        public String name() {
            return "POINT_DIRECT";
        }

        @Override
        public String descr() {
            return "Solve the direct geodesic problem." +
                    " 1st argument is source Point, 2nd is distance (in meters, can be negative) and 3rd is azimuth (degrees)";
        }
    }

    public static class PointLat extends Function.Unary<Double, PointEx> {
        @Override
        public Double call(Deque<Object> args) {
            return ((PointEx) args.pop()).getY();
        }

        @Override
        public String name() {
            return "POINT_LAT";
        }

        @Override
        public String descr() {
            return "Return latitude coordinate of a Point";
        }
    }

    public static class PointLon extends Function.Unary<Double, PointEx> {
        @Override
        public Double call(Deque<Object> args) {
            return ((PointEx) args.pop()).getX();
        }

        @Override
        public String name() {
            return "POINT_LON";
        }

        @Override
        public String descr() {
            return "Return longitude coordinate of a Point";
        }
    }

    public static class PoiRadius extends Function.Unary<Double, PointEx> {
        @Override
        public Double call(Deque<Object> args) {
            return ((PointEx) args.pop()).getRadius();
        }

        @Override
        public String name() {
            return "POI_RADIUS";
        }

        @Override
        public String descr() {
            return "Return radius of a POI Point";
        }
    }

    public static class PointInverse extends Function.Binary<ArrayWrap, PointEx, PointEx> {
        @Override
        public ArrayWrap call(Deque<Object> args) {
            PointEx s1 = (PointEx) args.pop();
            PointEx s2 = (PointEx) args.pop();

            GeodesicData gd = Geodesic.WGS84.Inverse(s1.getY(), s1.getX(), s2.getY(), s2.getX(), GeodesicMask.AZIMUTH | GeodesicMask.DISTANCE);
            return new ArrayWrap(new double[] {gd.s12, gd.azi1, gd.azi2});
        }

        @Override
        public String name() {
            return "POINT_INVERSE";
        }

        @Override
        public String descr() {
            return "Solve the inverse geodesic problem. Arguments are two Points." +
                    " Returns ARRAY of 3 elements: distance (in meters) between and azimuths of 1st and 2nd Points (degrees)";
        }
    }
}
