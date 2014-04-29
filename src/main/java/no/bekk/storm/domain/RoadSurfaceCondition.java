package no.bekk.storm.domain;

/**
 * Created by steffen stenersen on 20/04/14.
 */
public enum RoadSurfaceCondition {

    DRY(1),
    WET_OR_DAMP(2),
    SNOW(3),
    FROST_OR_ICE(4),
    FLOOD(5),
    OIL_OR_DIESEL(6),
    MUD(7),
    MISSING(-1);

    public final int code;

    RoadSurfaceCondition(int code) {
        this.code = code;
    }


}
