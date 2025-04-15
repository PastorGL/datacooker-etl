/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.data.spatial;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.Lineal;
import org.locationtech.jts.operation.BoundaryOp;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.stream.Collectors;

public class TrackSegment extends GeometryCollection implements Lineal, Iterable<Geometry>, SpatialRecord<TrackSegment> {
    protected PointEx centrePoint;

    TrackSegment() {
        super(null, FACTORY);
        setUserData(new HashMap<String, Object>());
    }

    public TrackSegment(Geometry[] geometries) {
        super(geometries, FACTORY);
        setUserData(new HashMap<String, Object>());
        this.centrePoint = getCentroid();
    }

    @Override
    public PointEx getCentroid() {
        if (centrePoint == null) {
            centrePoint = SpatialRecord.getCentroid(super.getCentroid(), getCoordinates());
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
        return new TrackSegment(revPoints);
    }

    protected TrackSegment copyInternal() {
        PointEx[] point = new PointEx[this.geometries.length];
        for (int i = 0; i < point.length; i++) {
            point[i] = (PointEx) this.geometries[i].copy();
        }
        return new TrackSegment(point);
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
        TrackSegment ts = new TrackSegment(geometries);
        ts.put(asIs());
        return ts;
    }

    @Override
    public int hashCode() {
        return super.hashCode() | asIs().hashCode();
    }

    @Override
    public String toString() {
        HashMap<String, Object> cp = new HashMap<>(asIs());
        cp.put(getGeometryType(), Arrays.stream(geometries).map(Geometry::toString).collect(Collectors.toList()));
        return cp.toString();
    }
}
