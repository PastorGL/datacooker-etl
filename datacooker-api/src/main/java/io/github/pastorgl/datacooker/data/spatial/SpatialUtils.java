/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.data.spatial;

import com.uber.h3core.H3Core;
import com.uber.h3core.LengthUnit;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

public class SpatialUtils implements Serializable {
    private int recursion = 1;
    private int resolution = 15;

    public static final H3Core H3;

    static {
        try {
            H3 = H3Core.newInstance();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private double radius;

    public SpatialUtils(double radius) {
        if (this.radius != radius) {
            for (int i = 15; ; i--) {
                double length = H3.getHexagonEdgeLengthAvg(i, LengthUnit.m);
                if (length > radius) {
                    recursion = (int) Math.floor(length / H3.getHexagonEdgeLengthAvg(i + 1, LengthUnit.m));
                    resolution = i + 1;
                    this.radius = radius;
                    return;
                }
            }
        }
    }

    public List<Long> getNeighbours(long h3index) {
        return H3.gridDisk(h3index, recursion);
    }

    public List<Long> getNeighbours(double lat, double lon) {
        return H3.gridDisk(H3.latLngToCell(lat, lon, resolution), recursion);
    }

    public long getHash(double lat, double lon) {
        return H3.latLngToCell(lat, lon, resolution);
    }

    public int getResolution() {
        return resolution;
    }
}
