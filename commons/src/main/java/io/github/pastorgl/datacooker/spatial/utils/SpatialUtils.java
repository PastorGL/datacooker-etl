/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.spatial.utils;

import io.github.pastorgl.datacooker.data.spatial.PointEx;
import com.uber.h3core.H3Core;
import com.uber.h3core.LengthUnit;
import net.sf.geographiclib.Geodesic;
import net.sf.geographiclib.GeodesicMask;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.Point;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class SpatialUtils implements Serializable {
    private int recursion = 1;
    private int resolution = 15;

    private static H3Core h3 = null;

    private double radius;

    public SpatialUtils(double radius) {
        setupH3();

        if (this.radius != radius) {
            for (int i = 15; ; i--) {
                double length = h3.getHexagonEdgeLengthAvg(i, LengthUnit.m);
                if (length > radius) {
                    recursion = (int) Math.floor(length / h3.getHexagonEdgeLengthAvg(i + 1, LengthUnit.m));
                    resolution = i + 1;
                    this.radius = radius;
                    return;
                }
            }
        }
    }

    public static PointEx getCentroid(Point centroid, Coordinate[] coordinates) {
        if (coordinates.length == 0) {
            return null;
        }

        Coordinate minX = new CoordinateXY(Double.POSITIVE_INFINITY, Double.NaN),
                minY = new CoordinateXY(Double.NaN, Double.POSITIVE_INFINITY),
                maxX = new CoordinateXY(Double.NEGATIVE_INFINITY, Double.NaN),
                maxY = new CoordinateXY(Double.NaN, Double.NEGATIVE_INFINITY);
        for (Coordinate c : coordinates) {
            if (c.x < minX.x) {
                minX = (Coordinate) c.clone();
            }
            if (c.x > maxX.x) {
                maxX = (Coordinate) c.clone();
            }
            if (c.y < minY.y) {
                minY = (Coordinate) c.clone();
            }
            if (c.y > maxY.y) {
                maxY = (Coordinate) c.clone();
            }
        }
        double radius = 0.D;
        for (Coordinate c : Arrays.asList(minX, minY, maxX, maxY)) {
            double r = Geodesic.WGS84.Inverse(c.y, c.x,
                    centroid.getY(), centroid.getX(), GeodesicMask.DISTANCE
            ).s12;
            if (r > radius) {
                radius = r;
            }
        }
        PointEx centrePoint = new PointEx(centroid);
        centrePoint.setRadius(radius);
        return centrePoint;
    }

    private void setupH3() {
        try {
            if (h3 == null) {
                h3 = H3Core.newInstance();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public List<Long> getNeighbours(long h3index) {
        setupH3();

        return h3.gridDisk(h3index, recursion);
    }

    public List<Long> getNeighbours(double lat, double lon) {
        setupH3();

        return h3.gridDisk(h3.latLngToCell(lat, lon, resolution), recursion);
    }

    public long getHash(double lat, double lon) {
        setupH3();

        return h3.latLngToCell(lat, lon, resolution);
    }

    public int getResolution() {
        return resolution;
    }
}
