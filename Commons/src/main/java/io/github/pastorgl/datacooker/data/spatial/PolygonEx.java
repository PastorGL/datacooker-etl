/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.data.spatial;

import io.github.pastorgl.datacooker.spatial.utils.SpatialUtils;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Polygon;

import java.util.HashMap;

public class PolygonEx extends Polygon implements SpatialRecord<PolygonEx> {
    final public PointEx centrePoint;

    public PolygonEx(Geometry polygon) {
        super(null, null, polygon.getFactory());

        if (polygon instanceof PolygonEx) {
            this.shell = ((PolygonEx) polygon).getExteriorRing();
            this.holes = ((PolygonEx) polygon).holes;
            this.centrePoint = ((PolygonEx) polygon).centrePoint;
            this.setUserData(new HashMap<>());
        } else {
            Polygon source = (Polygon) polygon;
            this.shell = source.getExteriorRing();
            this.holes = new LinearRing[source.getNumInteriorRing()];
            for (int i = 0; i < holes.length; i++) {
                holes[i] = source.getInteriorRingN(i);
            }

            this.centrePoint = getCentroid();
            this.setUserData(new HashMap<>());
        }
    }

    public PointEx getCentroid() {
        if (centrePoint != null) {
            return centrePoint;
        }
        return SpatialUtils.getCentroid(super.getCentroid(), getCoordinates());
    }

    @Override
    public String getGeometryType() {
        return "PolygonEx";
    }

    @Override
    public PolygonEx clone() {
        PolygonEx copy = new PolygonEx(this);
        copy.setUserData(new HashMap<>((HashMap<String, Object>) getUserData()));
        return copy;
    }
}

