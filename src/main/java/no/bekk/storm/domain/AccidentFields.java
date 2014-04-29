package no.bekk.storm.domain;

import backtype.storm.tuple.Fields;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by steffen stenersen on 20/04/14.
 */
public enum AccidentFields {

    ACCIDENT_INDEX(0),
    ACCIDENT_SEVERITY(6),
    NUMBER_OF_VEHICLES(7),
    NUMBER_OF_CASUALTIES(8),
    DATE(9),
    SPEED_LIMIT(17),
    LIGHT_CONDITIONS(25),
    WEATHER_CONDITIONS(26),
    ROAD_SURFACE_CONDITIONS(27);


    public final int index;

    AccidentFields(int index) {
        this.index = index;
    }

    public static Fields getFields() {
        List<String> fields = new ArrayList<String>();


        for (AccidentFields field : AccidentFields.values()) {
            fields.add(field.name());
        }

        return new Fields(fields);
    }

    public static Integer[] getIndices() {
        ArrayList<Integer> indices = new ArrayList<Integer>();

        for (AccidentFields field : AccidentFields.values()) {
            indices.add(field.index);
        }

        return indices.toArray(new Integer[indices.size()]);
    }

}
