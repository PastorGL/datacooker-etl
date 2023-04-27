/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.data.spatial;

import io.github.pastorgl.datacooker.spatial.utils.SpatialUtils;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Lineal;
import org.locationtech.jts.operation.BoundaryOp;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;

public class TrackSegment extends GeometryCollection implements Lineal, Iterable<Geometry>, SpatialRecord<TrackSegment> {
    protected PointEx centrePoint;

    TrackSegment() {
        super(null, FACTORY);
    }

    public TrackSegment(PointEx[] geometries, GeometryFactory factory) {
        super(geometries, factory);
        setUserData(new HashMap<String, Object>());
        this.centrePoint = getCentroid();
    }

    public TrackSegment(Geometry[] geometries, GeometryFactory factory) {
        super(geometries, factory);
        setUserData(new HashMap<String, Object>());
        this.centrePoint = getCentroid();
    }

    @Override
    public PointEx getCentroid() {
        if (centrePoint == null) {
            centrePoint = SpatialUtils.getCentroid(super.getCentroid(), getCoordinates());
        }
        return centrePoint;
    }

    public int getDimension() {
        return 1;
    }

    public int getBoundaryDimension() {
        return 0;
    }

    public String getGeometryType() {
        return "TrackSegment";
    }

    public Geometry getBoundary() {
        return (new BoundaryOp(this)).getBoundary();
    }

    public GeometryCollection reverse() {
        int nLines = geometries.length;
        PointEx[] revPoints = new PointEx[nLines];
        for (int i = 0; i < geometries.length; i++) {
            revPoints[nLines - 1 - i] = (PointEx) geometries[i].reverse();
        }
        return new TrackSegment(revPoints, getFactory());
    }

    protected TrackSegment copyInternal() {
        PointEx[] point = new PointEx[this.geometries.length];
        for (int i = 0; i < point.length; i++) {
            point[i] = (PointEx) this.geometries[i].copy();
        }
        return new TrackSegment(point, factory);
    }

    public boolean equalsExact(Geometry other, double tolerance) {
        if (!isEquivalentClass(other)) {
            return false;
        }
        return super.equalsExact(other, tolerance);
    }

    @Override
    public Iterator<Geometry> iterator() {
        return Arrays.stream(geometries).iterator();
    }

    public Geometry[] geometries() {
        return geometries;
    }

    @Override
    public TrackSegment clone() {
        TrackSegment ts = new TrackSegment(geometries, factory);
        ts.setUserData(new HashMap<>((HashMap<String, Object>) getUserData()));
        return ts;
    }

    @Override
    public int hashCode() {
        return super.hashCode() | getUserData().hashCode();
    }
}
