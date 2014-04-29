package no.bekk.storm.domain;

/**
 * Created by steffen stenersen on 20/04/14.
 */
public enum AccidentSeverity {
    FATAL(1),
    SERIOUS(2),
    SLIGHT(3);

    public final int code;

    AccidentSeverity(int code) {
        this.code = code;
    }

}
