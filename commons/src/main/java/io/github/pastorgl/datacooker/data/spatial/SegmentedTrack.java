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

public class SegmentedTrack extends GeometryCollection implements Lineal, Iterable<Geometry>, SpatialRecord<SegmentedTrack> {
    public final PointEx centrePoint;

    public SegmentedTrack(Geometry[] geometries, GeometryFactory factory) {
        super(geometries, factory);
        setUserData(new HashMap<String, Object>());
        this.centrePoint = getCentroid();
    }

    public SegmentedTrack(TrackSegment[] geometries, GeometryFactory factory) {
        super(geometries, factory);
        setUserData(new HashMap<String, Object>());
        this.centrePoint = getCentroid();
    }

    public PointEx getCentroid() {
        if (centrePoint != null) {
            return centrePoint;
        }
        return SpatialUtils.getCentroid(super.getCentroid(), getCoordinates());
    }

    public int getDimension() {
        return 1;
    }

    public int getBoundaryDimension() {
        return 0;
    }

    public String getGeometryType() {
        return "SegmentedTrack";
    }

    public Geometry getBoundary() {
        return (new BoundaryOp(this)).getBoundary();
    }

    public GeometryCollection reverse() {
        int nLines = geometries.length;
        TrackSegment[] revSegments = new TrackSegment[nLines];
        for (int i = 0; i < geometries.length; i++) {
            revSegments[nLines - 1 - i] = (TrackSegment) geometries[i].reverse();
        }
        return new SegmentedTrack(revSegments, getFactory());
    }

    protected SegmentedTrack copyInternal() {
        TrackSegment[] segments = new TrackSegment[this.geometries.length];
        for (int i = 0; i < segments.length; i++) {
            segments[i] = (TrackSegment) this.geometries[i].copy();
        }
        return new SegmentedTrack(segments, factory);
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
    public SegmentedTrack clone() {
        SegmentedTrack st = new SegmentedTrack(geometries, factory);
        st.setUserData(new HashMap<>((HashMap<String, Object>) getUserData()));
        return st;
    }
}
