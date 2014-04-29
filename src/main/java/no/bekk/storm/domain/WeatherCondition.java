package no.bekk.storm.domain;

public enum WeatherCondition {
    FINE_NO_HIGH_WINDS("1"),
    RAINING_NO_HIGH_WINDS("2"),
    SNOWING_NO_HIGH_WINDS("3"),
    FINE_HIGH_WINDS("4"),
    RAINING_HIGH_WINDS("5"),
    SNOWING_HIGH_WINDS("6"),
    FOG_OR_MIST("7"),
    OTHER("8"),
    UNKNOWN("9"),
    MISSING("-1");

    public final String code;

    WeatherCondition(String code) {
        this.code = code;
    }
}
