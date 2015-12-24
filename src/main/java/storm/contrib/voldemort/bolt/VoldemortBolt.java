package storm.contrib.voldemort.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import java.util.Map;
import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.client.StoreClientFactory;

/**
 * Storm Bolt to persist data into Voldemort.
 */
public class VoldemortBolt extends BaseRichBolt {

    private OutputCollector collector;
    private StoreClientFactory factory;
    private StoreClient client;
    private final String bootstrapUrl;
    private final String store;

    /**
     * Constructor initializes Voldemort configuration.
     *
     * @param bootstrapUrl the URL to Voldemort
     * @param store the store to put the data into
     */
    public VoldemortBolt(String bootstrapUrl, String store) {
        this.bootstrapUrl = bootstrapUrl;
        this.store = store;
    }

    /**
     * No fields to declare since this bolt only persists data.
     *
     * @param declarer the output fields declarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    /**
     * Initializes the connection to Voldemort.
     *
     * @param stormConf the Storm configuration
     * @param context the topology context
     * @param collector the output collector
     */
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        try {
            factory = new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls(bootstrapUrl));
            client = factory.getStoreClient(store);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * For every input tuple, gets the field name-value pairs and stores them
     * into Voldemort.
     *
     * @param input the input tuple
     */
    @Override
    public void execute(Tuple input) {
        Fields ff = input.getFields();
        for (String f : ff) {
            Object v = input.getValueByField(f);
            client.put(f, v);
        }
        collector.ack(input);
    }

    /**
     * Close the Voldemort store factory.
     */
    @Override
    public void cleanup() {
        if (factory != null) {
            factory.close();
        }
    }
}
