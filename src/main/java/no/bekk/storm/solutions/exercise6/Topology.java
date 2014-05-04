package no.bekk.storm.solutions.exercise6;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.Fields;
import no.bekk.storm.domain.AccidentFields;
import no.bekk.storm.domain.DataSource;
import no.bekk.storm.domain.VehicleFields;
import no.bekk.storm.solutions.filters.AgeFilter;
import no.bekk.storm.solutions.functions.PrintFunction;
import no.bekk.storm.spout.FileReaderSpout;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.testing.MemoryMapState;

import java.util.List;

public class Topology {
    public static void main(String[] args) throws Exception {
        TridentTopology topology = new TridentTopology();

        List<String> joinedFields = AccidentFields.getFields().toList();
        joinedFields.add(VehicleFields.ACCIDENT_INDEX.getField());

        Stream accidents = topology.newStream("accidents",
                new FileReaderSpout(DataSource.ACCIDENT, 30, AccidentFields.getFields(), AccidentFields.getIndices()))
                .project(new Fields(AccidentFields.ACCIDENT_INDEX.name(), AccidentFields.DAY_OF_WEEK.name()));

        Stream vehicle = topology.newStream("vehicle",
                new FileReaderSpout(DataSource.VEHICLE, 30, VehicleFields.getFields(), VehicleFields.getIndices()))
                .project(new Fields(VehicleFields.ACCIDENT_INDEX.getField(), VehicleFields.AGE_BAND_OF_DRIVER.getField()));

        topology.join(accidents, new Fields(AccidentFields.ACCIDENT_INDEX.name()), vehicle,
                new Fields(VehicleFields.ACCIDENT_INDEX.getField()), new Fields("ID", AccidentFields.DAY_OF_WEEK.name(), VehicleFields.AGE_BAND_OF_DRIVER.getField()))
                .each(new Fields(AccidentFields.DAY_OF_WEEK.name(), VehicleFields.AGE_BAND_OF_DRIVER.getField()), new AgeFilter())
                .groupBy(new Fields(AccidentFields.DAY_OF_WEEK.name()))
                .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("COUNT")).newValuesStream()
                .each(new Fields("DAY_OF_WEEK", "COUNT"), new PrintFunction(), new Fields());

        Config config = new Config();
        config.setDebug(true);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("accidents-topology", config, topology.build());

        Thread.sleep(10000);
    }
}
