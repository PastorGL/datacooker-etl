/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.data.spatial;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.github.pastorgl.datacooker.data.ObjMapper;
import io.github.pastorgl.datacooker.spatial.utils.SpatialUtils;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.impl.CoordinateArraySequence;

import java.util.Arrays;
import java.util.HashMap;

public class PolygonEx extends Polygon implements SpatialRecord<PolygonEx>, KryoSerializable {
    private PointEx centrePoint;

    public PolygonEx() {
        super(null, null, FACTORY);
        setUserData(new HashMap<String, Object>());
    }

    public PolygonEx(Geometry polygon) {
        super(null, null, polygon.getFactory());

        if (polygon instanceof PolygonEx) {
            shell = ((PolygonEx) polygon).getExteriorRing();
            holes = ((PolygonEx) polygon).holes;

            centrePoint = ((PolygonEx) polygon).centrePoint;
        } else {
            Polygon source = (Polygon) polygon;
            shell = source.getExteriorRing();
            holes = new LinearRing[source.getNumInteriorRing()];
            for (int i = 0; i < holes.length; i++) {
                holes[i] = source.getInteriorRingN(i);
            }

            centrePoint = getCentroid();
        }
        setUserData(new HashMap<String, Object>());
    }

    @Override
    public PointEx getCentroid() {
        if (centrePoint == null) {
            centrePoint = SpatialUtils.getCentroid(super.getCentroid(), getCoordinates());
        }
        return centrePoint;
    }

    @Override
    public String getGeometryType() {
        return "PolygonEx";
    }

    @Override
    public PolygonEx clone() {
        PolygonEx copy = new PolygonEx(this);
        copy.put(asIs());
        return copy;
    }

    @Override
    public void write(Kryo kryo, Output output) {
        try {
            var shellC = getExteriorRing().getCoordinates();
            output.writeInt(shellC.length);
            Arrays.stream(shellC).forEach(c -> {
                output.writeDouble(c.x);
                output.writeDouble(c.y);
            });
            int holes = getNumInteriorRing();
            output.writeInt(holes);
            for (int i = 0; i < holes; i++) {
                var hole = getInteriorRingN(i).getCoordinates();
                output.writeInt(hole.length);
                Arrays.stream(hole).forEach(c -> {
                    output.writeDouble(c.x);
                    output.writeDouble(c.y);
                });
            }
            byte[] arr = ObjMapper.BSON.writeValueAsBytes(asIs());
            output.writeInt(arr.length);
            output.write(arr, 0, arr.length);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void read(Kryo kryo, Input input) {
        try {
            int shellC = input.readInt();
            CoordinateArraySequence coordinates = new CoordinateArraySequence(shellC, 2);
            for (int i = 0; i < shellC; i++) {
                coordinates.setOrdinate(i, CoordinateSequence.X, input.readDouble());
                coordinates.setOrdinate(i, CoordinateSequence.Y, input.readDouble());
            }
            shell = FACTORY.createLinearRing(coordinates);
            int holeC = input.readInt();
            if (holeC > 0) {
                holes = new LinearRing[holeC];
                for (int i = 0; i < holeC; i++) {
                    int holeCC = input.readInt();
                    CoordinateArraySequence coordinatesH = new CoordinateArraySequence(holeCC, 2);
                    for (int j = 0; j < holeCC; j++) {
                        coordinatesH.setOrdinate(j, CoordinateSequence.X, input.readDouble());
                        coordinatesH.setOrdinate(j, CoordinateSequence.Y, input.readDouble());
                    }
                    holes[i] = FACTORY.createLinearRing(coordinatesH);
                }
            } else {
                holes = new LinearRing[0];
            }
            int length = input.readInt();
            byte[] bytes = input.readBytes(length);
            put(ObjMapper.BSON.readValue(bytes, HashMap.class));
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