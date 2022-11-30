/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.data.spatial;

import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;

import java.util.HashMap;

public class PointEx extends Point implements SpatialRecord<PointEx> {
    public PointEx(CoordinateSequence coordinates, GeometryFactory factory) {
        super(coordinates, factory);
        setUserData(new HashMap<String, Object>());
    }

    public PointEx(Geometry point) {
        super(((Point) point).getCoordinateSequence(), point.getFactory());
        setUserData(new HashMap<String, Object>());
    }

    public void setRadius(double radius) {
        this.getCoordinate().z = radius;
    }

    public double getRadius() {
        return this.getCoordinate().z;
    }

    @Override
    public String getGeometryType() {
        return "PointEx";
    }

    @Override
    public PointEx clone() {
        PointEx p = new PointEx(this);
        p.setUserData(new HashMap<>((HashMap<String, Object>) getUserData()));
        return p;
    }
}
