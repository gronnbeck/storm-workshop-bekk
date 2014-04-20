package no.bekk.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.Fields;
import no.bekk.storm.functions.PrintFunction;
import no.bekk.storm.spout.AccidentSpout;
import storm.trident.TridentTopology;

/**
 * Created by steffen stenersen on 20/04/14.
 */
public class AccidentTopology {
    public static void main(String[] args) throws Exception {
        TridentTopology topology = new TridentTopology();
        topology.newStream("accidents",
                new AccidentSpout()).each(new Fields("id", "value"), new PrintFunction(), new Fields("asdf"));


        Config config = new Config();
        config.setDebug(true);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("accidents-topology", config, topology.build());

        Thread.sleep(10000);
    }


}
