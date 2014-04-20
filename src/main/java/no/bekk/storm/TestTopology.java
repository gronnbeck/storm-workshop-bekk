package no.bekk.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import no.bekk.storm.spout.ActiveMQSpout;

/**
 * Created by steffen stenersen on 17/04/14.
 */
public class TestTopology {

    static TopologyBuilder builder;

    public static void main(String[] args) throws Exception {
        builder = new TopologyBuilder();
        builder.setSpout("spout", new ActiveMQSpout(), 1);


        Config config = new Config();
        config.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("activemq-topology", config, builder.createTopology());

        Thread.sleep(100000);

        cluster.shutdown();
    }
}
