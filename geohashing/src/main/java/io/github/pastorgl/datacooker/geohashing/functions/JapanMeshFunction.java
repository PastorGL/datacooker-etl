/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.geohashing.functions;

import io.github.pastorgl.datacooker.data.Record;
import org.apache.commons.math3.fraction.Fraction;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class JapanMeshFunction extends HasherFunction {
    private final Fraction LAT_HEIGHT_MESH1 = new Fraction(2, 3);
    private final Fraction LNG_WIDTH_MESH1 = new Fraction(1, 1);
    private final Fraction LAT_HEIGHT_MESH2 = LAT_HEIGHT_MESH1.divide(8);
    private final Fraction LNG_WIDTH_MESH2 = LNG_WIDTH_MESH1.divide(8);
    private final Fraction LAT_HEIGHT_MESH3 = LAT_HEIGHT_MESH2.divide(10);
    private final Fraction LNG_WIDTH_MESH3 = LNG_WIDTH_MESH2.divide(10);
    private final Fraction LAT_HEIGHT_MESH4 = LAT_HEIGHT_MESH3.divide(2);
    private final Fraction LNG_WIDTH_MESH4 = LNG_WIDTH_MESH3.divide(2);
    private final Fraction LAT_HEIGHT_MESH5 = LAT_HEIGHT_MESH4.divide(2);
    private final Fraction LNG_WIDTH_MESH5 = LNG_WIDTH_MESH4.divide(2);
    private final Fraction LAT_HEIGHT_MESH6 = LAT_HEIGHT_MESH5.divide(2);
    private final Fraction LNG_WIDTH_MESH6 = LNG_WIDTH_MESH5.divide(2);

    public JapanMeshFunction(int level) {
        super(level);
    }

    /**
     * Calculate code.
     *
     * @param lon longitude(decimal)
     * @param lat latitude(decimal)
     */
    public String getHash(double lat, double lon) {
        String code = "";

        // mesh level 1 : 4 characters ==============================
        int lat_1 = (int) (lat * 1.5) % 100;  // mesh1 lat
        int lng_1 = (int) (lon - 100);        // mesh1 lng

        // level 1 ==============================
        if (level >= 1) {
            // Mesh code format:
            code = String.format("%02d%02d", lat_1, lng_1);
        }

        // level 2 ==============================
        if (level < 2) return code;

        // mesh level 2 : append 2 additional characters
        int lat_2 = (int) ((lat -
                LAT_HEIGHT_MESH1.multiply(lat_1).doubleValue()) / LAT_HEIGHT_MESH2.doubleValue());
        int lng_2 = (int) ((lon - (lng_1 + 100)) / LNG_WIDTH_MESH2.doubleValue());

        // Mesh code format:  %02d%02d-%d%d
        code = String.format("%02d%02d%d%d", lat_1, lng_1, lat_2, lng_2);

        // level 3 ==============================
        if (level < 3) return code;

        // mesh level 3 : append 2 additional characters
        int lat_3 = (int) ((lat -
                LAT_HEIGHT_MESH1.multiply(lat_1).doubleValue() -
                LAT_HEIGHT_MESH2.multiply(lat_2).doubleValue())
                / LAT_HEIGHT_MESH3.doubleValue());
        int lng_3 = (int) ((lon - (lng_1 + 100) -
                LNG_WIDTH_MESH2.multiply(lng_2).doubleValue())
                / LNG_WIDTH_MESH3.doubleValue());

        // Mesh code format:  %02d%02d-%d%d-%d%d
        code = String.format("%02d%02d%d%d%d%d", lat_1, lng_1, lat_2, lng_2, lat_3, lng_3);

        // level 4 ==============================
        if (level < 4) return code;

        // mesh level 4 : append 2 additional characters
        int lat_4 = (int) ((lat -
                LAT_HEIGHT_MESH1.multiply(lat_1).doubleValue() -
                LAT_HEIGHT_MESH2.multiply(lat_2).doubleValue() -
                LAT_HEIGHT_MESH3.multiply(lat_3).doubleValue())
                / LAT_HEIGHT_MESH4.doubleValue());
        int lng_4 = (int) ((lon -
                (lng_1 + 100) -
                LNG_WIDTH_MESH2.multiply(lng_2).doubleValue() -
                LNG_WIDTH_MESH3.multiply(lng_3).doubleValue())
                / LNG_WIDTH_MESH4.doubleValue());

        // Mesh code format: %02d%02d-%d%d-%d%d-%d
        code = String.format("%02d%02d%d%d%d%d%d", lat_1, lng_1, lat_2, lng_2, lat_3, lng_3,
                composeCode(lat_4, lng_4));

        // level 5 ==============================
        if (level < 5) return code;

        // mesh level 5 : append 1 additional characters
        int lat_5 = (int) ((lat -
                LAT_HEIGHT_MESH1.multiply(lat_1).doubleValue() -
                LAT_HEIGHT_MESH2.multiply(lat_2).doubleValue() -
                LAT_HEIGHT_MESH3.multiply(lat_3).doubleValue() -
                LAT_HEIGHT_MESH4.multiply(lat_4).doubleValue())
                / LAT_HEIGHT_MESH5.doubleValue());
        int lng_5 = (int) ((lon -
                (lng_1 + 100) -
                LNG_WIDTH_MESH2.multiply(lng_2).doubleValue() -
                LNG_WIDTH_MESH3.multiply(lng_3).doubleValue() -
                LNG_WIDTH_MESH4.multiply(lng_4).doubleValue())
                / LNG_WIDTH_MESH5.doubleValue());

        // Mesh code format:  %02d%02d-%d%d-%d%d-%d-%d
        code = String.format("%02d%02d%d%d%d%d%d%d", lat_1, lng_1, lat_2, lng_2, lat_3, lng_3,
                composeCode(lat_4, lng_4),
                composeCode(lat_5, lng_5));

        // level 6 ==============================
        if (level < 6) return code;
        // mesh level 6 : append 1 additional characters
        int lat_6 = (int) ((lat -
                LAT_HEIGHT_MESH1.multiply(lat_1).doubleValue() -
                LAT_HEIGHT_MESH2.multiply(lat_2).doubleValue() -
                LAT_HEIGHT_MESH3.multiply(lat_3).doubleValue() -
                LAT_HEIGHT_MESH4.multiply(lat_4).doubleValue() -
                LAT_HEIGHT_MESH5.multiply(lat_5).doubleValue())
                / LAT_HEIGHT_MESH6.doubleValue());
        int lng_6 = (int) ((lon -
                (lng_1 + 100) -
                LNG_WIDTH_MESH2.multiply(lng_2).doubleValue() -
                LNG_WIDTH_MESH3.multiply(lng_3).doubleValue() -
                LNG_WIDTH_MESH4.multiply(lng_4).doubleValue() -
                LNG_WIDTH_MESH5.multiply(lng_5).doubleValue())
                / LNG_WIDTH_MESH6.doubleValue());

        // Mesh code format:  %02d%02d-%d%d-%d%d-%d-%d-%d
        code = String.format("%02d%02d%d%d%d%d%d%d%d", lat_1, lng_1, lat_2, lng_2, lat_3, lng_3,
                composeCode(lat_4, lng_4),
                composeCode(lat_5, lng_5),
                composeCode(lat_6, lng_6));

        return code;
    }


    /**
     * get code index under level 4 mesh
     *
     * @return mesh code index
     */
    private int composeCode(int latIndex, int lonIndex) {
        if (latIndex == 0 && lonIndex == 0) {
            return 1;
        } else if (latIndex == 0 && lonIndex == 1) {
            return 2;
        } else if (latIndex == 1 && lonIndex == 0) {
            return 3;
        } else if (latIndex == 1 && lonIndex == 1) {
            return 4;
        } else {
            return 0;
        }
    }

    @Override
    public Iterator<Tuple2<String, Record>> call(Iterator<Tuple3<Double, Double, Record>> signals) {
        List<Tuple2<String, Record>> ret = new ArrayList<>();
        while (signals.hasNext()) {
            Tuple3<Double, Double, Record> signal = signals.next();

            ret.add(new Tuple2<>(getHash(signal._1(), signal._2()), signal._3()));
        }

        return ret.iterator();
    }
}
