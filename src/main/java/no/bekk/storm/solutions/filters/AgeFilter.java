package no.bekk.storm.solutions.filters;

import no.bekk.storm.domain.VehicleFields;
import storm.trident.operation.Filter;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.Map;

public class AgeFilter implements Filter {

    @Override
    public void prepare(Map map, TridentOperationContext tridentOperationContext) {

    }

    @Override
    public void cleanup() {

    }

    @Override
    public boolean isKeep(TridentTuple tuple) {
        int ageBand = Integer.parseInt(tuple.getStringByField(VehicleFields.AGE_BAND_OF_DRIVER.fieldName));
        return ageBand <= 5;
    }
}