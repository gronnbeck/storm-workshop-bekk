package no.bekk.storm.messageQueue;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.io.BufferedReader;
import java.io.FileReader;

/**
 * Created by steffen stenersen on 16/04/14.
 */
public class EventProducer {

    public static void main(String[] args) throws Exception {
        thread(new HelloWorldProducer(), false);
//        Thread.sleep(1000);
//        thread(new HelloWorldConsumer(), false);
//        thread(new HelloWorldProducer(), false);
//        thread(new HelloWorldConsumer(), false);
//        thread(new HelloWorldProducer(), false);
//        Thread.sleep(1000);
//        thread(new HelloWorldConsumer(), false);
//        thread(new HelloWorldProducer(), false);
//        thread(new HelloWorldConsumer(), false);
//        thread(new HelloWorldConsumer(), false);
//        thread(new HelloWorldProducer(), false);
//        thread(new HelloWorldProducer(), false);
//        Thread.sleep(1000);
//        thread(new HelloWorldProducer(), false);
//        thread(new HelloWorldConsumer(), false);
//        thread(new HelloWorldConsumer(), false);
//        thread(new HelloWorldProducer(), false);
//        thread(new HelloWorldConsumer(), false);
//        thread(new HelloWorldProducer(), false);
//        thread(new HelloWorldConsumer(), false);
//        thread(new HelloWorldProducer(), false);
//        thread(new HelloWorldConsumer(), false);
//        thread(new HelloWorldConsumer(), false);
//        thread(new HelloWorldProducer(), false);
    }

    static ActiveMQConnectionFactory activeMQConnectionFactory;

    public static void thread(Runnable runnable, boolean daemon) {
        Thread brokerThread = new Thread(runnable);
        brokerThread.setDaemon(daemon);
        brokerThread.start();
    }

    public static class HelloWorldProducer implements Runnable {
        public void run() {
            try {

                activeMQConnectionFactory = new ActiveMQConnectionFactory("vm://localhost");

                Connection connection = activeMQConnectionFactory.createConnection();
                connection.start();

                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                Destination destination = session.createQueue("TEST.FOO");

                MessageProducer producer = session.createProducer(destination);
                producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

                String input;
                String path2  = System.getProperty("user.dir");

                FileReader file = new FileReader(path2 + "/Stats19-Data1979-2004/Accidents7904.csv");
                BufferedReader br = new BufferedReader(file);

                while((input = br.readLine()) != null) {
                    System.out.println(input);
                    TextMessage message = session.createTextMessage(input);
                    producer.send(message);
                    Thread.currentThread().sleep(1000);
                }




            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
