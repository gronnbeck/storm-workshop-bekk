package no.bekk.storm.solutions.filters;

import no.bekk.storm.domain.AccidentFields;
import no.bekk.storm.domain.WeatherCondition;
import storm.trident.operation.Filter;
import storm.trident.operation.Function;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.Map;

public class SnowFilter implements Filter {

    @Override
    public void prepare(Map map, TridentOperationContext tridentOperationContext) {

    }

    @Override
    public void cleanup() {

    }

    @Override
    public boolean isKeep(TridentTuple tuple) {
        String weather = tuple.getStringByField(AccidentFields.WEATHER_CONDITIONS.name());
        return weather.equals(WeatherCondition.SNOWING_HIGH_WINDS.code) || weather.equals(WeatherCondition.SNOWING_NO_HIGH_WINDS.code);
    }
}
