package no.bekk.storm.domain;

/**
 * Created by steffen stenersen on 20/04/14.
 */
public enum DataSource {
    ACCIDENT("Accidents7904.csv"),
    VEHICLE("Vehicles7904.csv"),
    CASUALTY("Casualty7904.csv");

    public final String filename;

    DataSource(String filename) {
        this.filename = filename;
    }
}
