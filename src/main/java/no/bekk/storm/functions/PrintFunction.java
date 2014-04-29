package no.bekk.storm.functions;

import storm.trident.operation.Function;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.Map;

public class PrintFunction implements Function {
    @Override
    public void execute(TridentTuple tuple, TridentCollector tridentCollector) {
        System.out.println(tuple);
    }

    @Override
    public void prepare(Map map, TridentOperationContext tridentOperationContext) {

    }

    @Override
    public void cleanup() {

    }
}
