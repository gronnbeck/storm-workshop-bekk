package no.bekk.storm.functions;

import backtype.storm.tuple.Values;
import no.bekk.storm.domain.AccidentFields;
import no.bekk.storm.domain.DayOfWeek;
import no.bekk.storm.domain.WeatherCondition;
import storm.trident.operation.Function;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.Map;

public class CodeMapperFunction implements Function {
    @Override
    public void execute(TridentTuple tuple, TridentCollector tridentCollector) {
        String dayOfWeek = tuple.getStringByField(AccidentFields.DAY_OF_WEEK.name());
        tridentCollector.emit(new Values(DayOfWeek.fromCode(dayOfWeek)));
    }

    @Override
    public void prepare(Map map, TridentOperationContext tridentOperationContext) {

    }

    @Override
    public void cleanup() {

    }
}
