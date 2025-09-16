/**
 * Copyright (C) 2025 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.spatial.functions;

import io.github.pastorgl.datacooker.data.spatial.PolygonEx;
import org.locationtech.jts.geom.Coordinate;
import org.wololo.geojson.Polygon;

public class PolygonConverter {
    public static double[][] apply(Coordinate[] coordinates) {
        double[][] array = new double[coordinates.length][];
        for (int i = 0; i < coordinates.length; i++) {
            array[i] = new double[]{coordinates[i].x, coordinates[i].y};
        }
        return array;
    }

    public static Polygon convert(PolygonEx poly) {
        int size = poly.getNumInteriorRing() + 1;
        double[][][] rings = new double[size][][];
        rings[0] = apply(poly.getExteriorRing().getCoordinates());
        for (int i = 0; i < size - 1; i++) {
            rings[i + 1] = apply(poly.getInteriorRingN(i).getCoordinates());
        }

        return new Polygon(rings);
    }
}
