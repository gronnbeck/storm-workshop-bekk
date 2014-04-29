package no.bekk.storm.domain;

import backtype.storm.tuple.Fields;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by steffen stenersen on 20/04/14.
 */
public enum CasualtyFields {

    ACCIDENT_INDEX(0),
    VEHICLE_REFERENCE(1),
    CASUALTY_REFERENCE(2),
    CASUALTY_CLASS(3),
    SEX_OF_CASUALTY(4),
    AGE_BAND_OF_CASUALTY(5),
    CASUALTY_SEVERITY(6),
    PEDESTRIAN_LOCATION(7),
    PEDESTRIAN_MOVEMENT(8),
    CAR_PASSENGER(9),
    BUS_OR_COACH_PASSENGER(10),
    CASUALTY_TYPE(12);


    public final int index;

    CasualtyFields(int index) {
        this.index = index;
    }

    public static Fields getFields() {
        List<String> fields = new ArrayList<String>();


        for (CasualtyFields field : CasualtyFields.values()) {
            fields.add(field.name());
        }

        return new Fields(fields);
    }

    public static Integer[] getIndices() {
        ArrayList<Integer> indices = new ArrayList<Integer>();

        for (CasualtyFields field : CasualtyFields.values()) {
            indices.add(field.index);
        }

        return indices.toArray(new Integer[indices.size()]);
    }

}
