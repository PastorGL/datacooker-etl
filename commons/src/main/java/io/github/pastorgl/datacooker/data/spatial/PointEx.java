/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.data.spatial;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.locationtech.jts.algorithm.Centroid;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.impl.CoordinateArraySequence;

import java.util.HashMap;

public class PointEx extends Point implements SpatialRecord<PointEx>, KryoSerializable {
    public PointEx() {
        super(new CoordinateArraySequence(1), FACTORY);
        setUserData(new HashMap<String, Object>());
    }

    public PointEx(CoordinateSequence coordinates) {
        super(coordinates, FACTORY);
        setUserData(new HashMap<String, Object>());
    }

    public PointEx(Geometry point) {
        super(new CoordinateArraySequence(new Coordinate[]{Centroid.getCentroid(point)}), FACTORY);
        setUserData(new HashMap<String, Object>());
    }

    public void setRadius(double radius) {
        this.getCoordinate().z = radius;
    }

    public double getRadius() {
        return this.getCoordinate().z;
    }

    @Override
    public PointEx getCentroid() {
        return this;
    }

    @Override
    public String getGeometryType() {
        return "PointEx";
    }

    @Override
    public PointEx clone() {
        PointEx p = new PointEx(this);
        p.put(asIs());
        return p;
    }

    @Override
    public void write(Kryo kryo, Output output) {
        try {
            Coordinate c = getCoordinate();
            output.writeDouble(c.x);
            output.writeDouble(c.y);
            output.writeDouble(c.z);
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
            Coordinate c = getCoordinate();
            c.x = input.readDouble();
            c.y = input.readDouble();
            c.z = input.readDouble();
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
