package no.bekk.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.Fields;
import no.bekk.storm.domain.AccidentFields;
import no.bekk.storm.domain.DataSource;
import no.bekk.storm.solutions.functions.PrintFunction;
import no.bekk.storm.spout.FileReaderSpout;
import storm.trident.TridentTopology;

public class ExampleTopology {
    public static void main(String[] args) throws Exception {
        TridentTopology topology = new TridentTopology();

        topology.newStream("accidents",
                    new FileReaderSpout(DataSource.ACCIDENT, 30, AccidentFields.getFields(), AccidentFields.getIndices()))
                .each(new Fields(AccidentFields.DAY_OF_WEEK.name()), new PrintFunction(), new Fields());

        Config config = new Config();
        config.setDebug(true);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("accidents-topology", config, topology.build());

        Thread.sleep(10000);
    }


}
