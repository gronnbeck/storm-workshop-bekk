package no.bekk.storm.domain;

public enum DayOfWeek {
    SUNDAY("1"),
    MONDAY("2"),
    TUESDAY("3"),
    WEDNESDAY("4"),
    THURSDAY("5"),
    FRIDAY("6"),
    SATURDAY("7");

    private String code;

    private DayOfWeek(String code) {
        this.code = code;
    }

    public static String fromCode(String code) {
        for (DayOfWeek dayOfWeek : DayOfWeek.values()) {
            if(dayOfWeek.code.equals(code)) {
                return dayOfWeek.name();
            }
        }
        return null;
    }

}
