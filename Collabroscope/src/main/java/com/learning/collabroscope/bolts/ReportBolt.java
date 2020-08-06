package collabroscope.bolts;

/**
 * Created by Subbu on 2/12/16.
 */

import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

/**
 * A bolt that prints the word and count to redis
 */
public class ReportBolt extends BaseRichBolt {
    // place holder to keep the connection to redis
    private transient RedisConnection<String, String> redis;

    @Override
    public void prepare(
            Map map,
            TopologyContext topologyContext,
            OutputCollector outputCollector) {
        // instantiate a redis connection
        RedisClient client = new RedisClient("localhost", 6379);

        // initiate the actual connection
        redis = client.connect();
    }

    @Override
    public void execute(Tuple tuple) {
        // access the second column 'count'
        String tweetUser = tuple.getStringByField("tweet-user");

        System.out.println("Tweet User: " + tweetUser);

        // publish the word count to redis using word as the key
        redis.publish("WordCountTopology", tweetUser + "|" + tweetUser.length());
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // nothing to add - since it is the final bolt
    }
}
