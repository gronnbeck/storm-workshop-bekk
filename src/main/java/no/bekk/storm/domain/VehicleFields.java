package no.bekk.storm.domain;

import backtype.storm.tuple.Fields;

import java.util.ArrayList;
import java.util.List;

public enum VehicleFields {

    ACCIDENT_INDEX(0, "V_ACCIDENT_INDEX"),
    VEHICLE_REFERENCE(1),
    VEHICLE_TYPE(2),
    TOWING_AND_ARTICULATION(3),
    VEHICLE_MANOEUVRE(4),
    SEX_OF_DRIVER(14),
    AGE_BAND_OF_DRIVER(15);


    public final int index;
    public final String fieldName;

    VehicleFields(int index) {
        this.index = index;
        this.fieldName = name();
    }

    VehicleFields(int index, String name) {
        this.index = index;
        this.fieldName = name;
    }

    public String getField() {
        return this.fieldName;
    }

    public static Fields getFields() {
        List<String> fields = new ArrayList<String>();


        for (VehicleFields field : VehicleFields.values()) {
            fields.add(field.fieldName);
        }

        return new Fields(fields);
    }

    public static Integer[] getIndices() {
        ArrayList<Integer> indices = new ArrayList<Integer>();

        for (VehicleFields field : VehicleFields.values()) {
            indices.add(field.index);
        }

        return indices.toArray(new Integer[indices.size()]);
    }
}
