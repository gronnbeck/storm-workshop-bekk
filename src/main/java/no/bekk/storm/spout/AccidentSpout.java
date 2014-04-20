package no.bekk.storm.spout;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Created by steffen stenersen on 20/04/14.
 */
public class AccidentSpout implements IBatchSpout {

    private LinkedBlockingDeque<String> queue;
    int batchSize;
    BufferedReader br;

    public AccidentSpout() {
        this.batchSize = 5;
    }

    @Override
    public void open(Map map, TopologyContext topologyContext) {
        queue = new LinkedBlockingDeque<String>();
        try {
            String path  = System.getProperty("user.dir");

            FileReader file = new FileReader(path + "/Stats19-Data1979-2004/Accidents7904.csv");
            br = new BufferedReader(file);


        } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
    }

    @Override
    public void emitBatch(long batchId, TridentCollector tridentCollector) {
        System.out.println("Batch size: " + batchSize);
        String input;

        int read = 0;

        try {
            while((input = br.readLine()) != null && read < batchSize) {
                queue.offer(input);
                read++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        for(int i = 0; i < batchSize; i++) {
            String message = queue.poll();
            if(message == null)
                Utils.sleep(50);
            else {
                String[] msgArray = message.split(",");
                Values values = new Values(msgArray[0], msgArray[1]);

                tridentCollector.emit(values);
            }
        }
    }

    @Override
    public void ack(long l) {

    }

    @Override
    public void close() {

    }

    @Override
    public Map getComponentConfiguration() {
        return null;
    }

    @Override
    public Fields getOutputFields() {
        return new Fields("id", "value");
    }

    public static void main(String[] args) throws IOException {
        AccidentSpout spout = new AccidentSpout();
        spout.open(null, null);
        for(int i = 0; i < 30; i++)
            System.out.println(spout.queue.poll());
    }


}
