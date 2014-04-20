package no.bekk.storm.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQMessageConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Created by steffen stenersen on 17/04/14.
 */
public class ActiveMQSpout extends BaseRichSpout implements MessageListener, ExceptionListener {

    private static final Logger LOG = LoggerFactory.getLogger(ActiveMQSpout.class);

    ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("vm://localhost");
    ActiveMQMessageConsumer messageConsumer;
    private SpoutOutputCollector collector;
    private LinkedBlockingDeque<Message> queue;


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
        Connection connection = null;
        try {
            connection = connectionFactory.createConnection();
            connection.start();
            connection.setExceptionListener(this);

            queue = new LinkedBlockingDeque<Message>();


            // Create a Session
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // Create the destination (Topic or Queue)
            Destination destination = session.createQueue("TEST.FOO");

            // Create a MessageConsumer from the Session to the Topic or Queue
            MessageConsumer consumer = session.createConsumer(destination);

            consumer.setMessageListener(this);
            connection.start();

        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void nextTuple() {
        Message message = queue.poll();
        if(message == null)
            Utils.sleep(50);
        else {
            LOG.debug("sending tuple: " + message);

            Values values = new Values("sasdsd", "asd");

            this.collector.emit(values);
        }
        //read from internal queue
        //collection emit
    }

    @Override
    public void onException(JMSException exception) {
        exception.printStackTrace();
    }

    @Override
    public void onMessage(Message message) {
        try {
        LOG.debug("Queuing msg [ " + message.getJMSMessageID() + " ]");
        } catch (JMSException e) {
        }

        this.queue.offer(message);
    }
}
