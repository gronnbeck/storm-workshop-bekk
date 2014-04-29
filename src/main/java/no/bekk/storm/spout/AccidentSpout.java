package no.bekk.storm.spout;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import com.sun.java.swing.plaf.windows.resources.windows_es;
import no.bekk.storm.domain.AccidentFields;
import no.bekk.storm.domain.DataSource;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;

public class AccidentSpout implements IBatchSpout {

    private LinkedBlockingDeque<String> queue;
    private DataSource dataSource;
    int batchSize;
    private Fields outputFields;
    private Integer[] indices;
    BufferedReader br;


    public AccidentSpout(DataSource dataSource, int batchSize, Fields outputFields, Integer[] indices) {
        this.dataSource = dataSource;
        this.batchSize = batchSize;
        this.outputFields = outputFields;
        this.indices = indices;
    }

    @Override
    public void open(Map map, TopologyContext topologyContext) {
        queue = new LinkedBlockingDeque<String>();
        try {
            String path  = System.getProperty("user.dir");

            FileReader file = new FileReader(path + "/datasett/" + dataSource.filename);
            br = new BufferedReader(file);


        } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
    }

    @Override
    public void emitBatch(long batchId, TridentCollector tridentCollector) {
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

                ArrayList<String> vals = new ArrayList<String>();
                for (Integer index : indices) {
                    vals.add(msgArray[index]);
                }

                Values values = new Values(msgArray[0], msgArray[6], msgArray[7], msgArray[8], msgArray[9], msgArray[17],
                        msgArray[25],msgArray[26],msgArray[27]);


                tridentCollector.emit(new Values(vals.toArray()));
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
        return outputFields;
    }
}
