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

/**
 * Created by steffen stenersen on 20/04/14.
 */
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

            FileReader file = new FileReader(path + "/Stats19-Data1979-2004/" + dataSource.filename);
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
        //0(accident_index), Accident_severity(6), num_vehiceles(7), num_casulties(8), Date(9), Speed limit(17),
        //Light_Conditions(25),Weather_Conditions(26), road_surface_conditions(27)
        //Accident_Index,Location_Easting_OSGR,Location_Northing_OSGR,Longitude,Latitude,Police_Force,Accident_Severity,
        //        Number_of_Vehicles,Number_of_Casualties,Date,Day_of_Week,Time,Local_Authority_(District),Local_Authority_(Highway),
        //        1st_Road_Class,1st_Road_Number,Road_Type,Speed_limit,Junction_Detail,Junction_Control,2nd_Road_Class,2nd_Road_Number,
        //        Pedestrian_Crossing-Human_Control,Pedestrian_Crossing-Physical_Facilities,Light_Conditions,Weather_Conditions,
        //        Road_Surface_Conditions,Special_Conditions_at_Site,Carriageway_Hazards,Urban_or_Rural_Area,
        //        Did_Police_Officer_Attend_Scene_of_Accident,LSOA_of_Accident_Location
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

    public static void main(String[] args) throws IOException {
        AccidentSpout spout = new AccidentSpout(DataSource.ACCIDENT, 5, new Fields(), AccidentFields.getIndices());
        spout.open(null, null);
        for(int i = 0; i < 30; i++)
            System.out.println(spout.queue.poll());
    }


}
