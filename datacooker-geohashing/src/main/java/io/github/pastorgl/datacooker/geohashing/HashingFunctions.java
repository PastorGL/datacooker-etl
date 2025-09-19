/**
 * Copyright (C) 2024 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.geohashing;

import io.github.pastorgl.datacooker.data.spatial.SpatialRecord;
import io.github.pastorgl.datacooker.scripting.Evaluator;
import io.github.pastorgl.datacooker.scripting.Function;
import io.github.pastorgl.datacooker.data.spatial.SpatialUtils;
import org.locationtech.jts.geom.Point;

import java.util.Deque;

@SuppressWarnings("unused")
public class HashingFunctions {
    public static class H3 extends Function.Ternary<String, Integer, Double, Double> {
        @Override
        public String call(Deque<Object> args) {
            try {
                int level = Evaluator.popInt(args);
                double lat = Evaluator.popDouble(args);
                double lon = Evaluator.popDouble(args);
                return SpatialUtils.H3.latLngToCellAddress(lat, lon, level);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public String name() {
            return "H3";
        }

        @Override
        public String descr() {
            return "Return an H3 hash of level set by 1st argument for latitude and longitude given as 2nd and 3rd";
        }
    }

    public static class H3SpatialRecord extends Function.RecordObject<String, SpatialRecord<?>> {
        @Override
        public String call(Deque<Object> args) {
            try {
                Point rec = ((SpatialRecord<?>) args.pop()).getCentroid();
                int level = Evaluator.popInt(args);
                return SpatialUtils.H3.latLngToCellAddress(rec.getY(), rec.getX(), level);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public String name() {
            return "REC_H3";
        }

        @Override
        public String descr() {
            return "Return an H3 hash of specified level for any Spatial type record";
        }
    }

    public static class H3GEO extends Function.Binary<String, SpatialRecord<?>, Integer> {
        @Override
        public String call(Deque<Object> args) {
            try {
                Point rec = ((SpatialRecord<?>) args.pop()).getCentroid();
                int level = Evaluator.popInt(args);
                return SpatialUtils.H3.latLngToCellAddress(rec.getY(), rec.getX(), level);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public String name() {
            return "GEO_H3";
        }

        @Override
        public String descr() {
            return "For any Spatial type Object return its H3 hash of specified level";
        }
    }
}
