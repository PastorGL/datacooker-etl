/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.data.spatial;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.github.pastorgl.datacooker.spatial.utils.SpatialUtils;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.Lineal;
import org.locationtech.jts.operation.BoundaryOp;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;

public class SegmentedTrack extends GeometryCollection implements Lineal, Iterable<Geometry>, SpatialRecord<SegmentedTrack>, KryoSerializable {
    public PointEx centrePoint;

    public SegmentedTrack() {
        super(null, FACTORY);
        setUserData(new HashMap<String, Object>());
    }

    public SegmentedTrack(Geometry[] geometries) {
        super(geometries, FACTORY);
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
        return new SegmentedTrack(revSegments);
    }

    protected SegmentedTrack copyInternal() {
        TrackSegment[] segments = new TrackSegment[this.geometries.length];
        for (int i = 0; i < segments.length; i++) {
            segments[i] = (TrackSegment) this.geometries[i].copy();
        }
        return new SegmentedTrack(segments);
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
        SegmentedTrack st = new SegmentedTrack(geometries);
        st.put(asIs());
        return st;
    }

    @Override
    public void write(Kryo kryo, Output output) {
        try {
            var segC = geometries.length;
            output.writeInt(segC);
            for (Geometry geometry : geometries) {
                TrackSegment trk = (TrackSegment) geometry;
                var pointC = trk.getNumGeometries();
                output.writeInt(pointC);
                Arrays.stream(trk.geometries()).forEach(c -> {
                    PointEx p = (PointEx) c;
                    p.write(kryo, output);
                });
                byte[] arr = BSON.writeValueAsBytes(trk.asIs());
                output.writeInt(arr.length);
                output.write(arr, 0, arr.length);
            }
            byte[] arr = BSON.writeValueAsBytes(asIs());
            output.writeInt(arr.length);
            output.write(arr, 0, arr.length);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void read(Kryo kryo, Input input) {
        try {
            int segC = input.readInt();
            geometries = new TrackSegment[segC];
            for (int i = 0; i < segC; i++) {
                int pointC = input.readInt();
                PointEx[] points = new PointEx[pointC];
                for (int j = 0; j < pointC; j++) {
                    points[j] = new PointEx();
                    points[j].read(kryo, input);
                }
                geometries[i] = new TrackSegment(points);
                int length = input.readInt();
                byte[] bytes = input.readBytes(length);
                geometries[i].setUserData(BSON.readValue(bytes, HashMap.class));
            }
            int length = input.readInt();
            byte[] bytes = input.readBytes(length);
            put(BSON.readValue(bytes, HashMap.class));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int hashCode() {
        return super.hashCode() | asIs().hashCode();
    }

    @Override
    public String toString() {
        HashMap<String, Object> cp = new HashMap<>(asIs());
        cp.put(getGeometryType(), toText());
        return cp.toString();
    }
}
