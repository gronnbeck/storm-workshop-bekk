package no.bekk.storm.domain;

public enum AccidentSeverity {
    FATAL(1),
    SERIOUS(2),
    SLIGHT(3);

    public final int code;

    AccidentSeverity(int code) {
        this.code = code;
    }

}
