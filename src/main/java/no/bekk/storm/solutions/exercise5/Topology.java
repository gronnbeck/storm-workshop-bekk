package no.bekk.storm.solutions.exercise5;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.Fields;
import no.bekk.storm.domain.AccidentFields;
import no.bekk.storm.domain.DataSource;
import no.bekk.storm.solutions.filters.SnowFilter;
import no.bekk.storm.solutions.functions.DayOfWeekMapperFunction;
import no.bekk.storm.solutions.functions.PrintFunction;
import no.bekk.storm.spout.FileReaderSpout;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.testing.MemoryMapState;

public class Topology {
    public static void main(String[] args) throws Exception {
        TridentTopology topology = new TridentTopology();

        topology.newStream("accidents",
                    new FileReaderSpout(DataSource.ACCIDENT, 30, AccidentFields.getFields(), AccidentFields.getIndices()))
                .each(AccidentFields.getFields(), new SnowFilter())
                .groupBy(new Fields(AccidentFields.DAY_OF_WEEK.name()))
                .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count")).newValuesStream()
                .parallelismHint(6)
                .each(new Fields(AccidentFields.DAY_OF_WEEK.name(), "count"), new DayOfWeekMapperFunction(), new Fields("dayName"))
                .each(new Fields("dayName", "count"), new PrintFunction(), new Fields());

        Config config = new Config();
        config.setDebug(true);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("accidents-topology", config, topology.build());

        Thread.sleep(10000);
    }
}
