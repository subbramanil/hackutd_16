package collabroscope.spouts;

import org.apache.storm.Config;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * A spout that uses Twitter streaming API for continuously
 * getting tweets
 */
public class TweetSpout extends BaseRichSpout {
    // Twitter API authentication credentials
    String custkey, custsecret;
    String accesstoken, accesssecret;

    // To output tuples from spout to the next stage bolt
    SpoutOutputCollector collector;

    // Twitter4j - twitter stream to get tweets
    TwitterStream twitterStream;

    // Shared queue for getting buffering tweets received
    LinkedBlockingQueue<String> queue = null;

    /**
     * Constructor for tweet spout that accepts the credentials
     */
    public TweetSpout(
            String key,
            String secret,
            String token,
            String tokensecret) {
        custkey = key;
        custsecret = secret;
        accesstoken = token;
        accesssecret = tokensecret;
    }

    @Override
    public void open(
            Map map,
            TopologyContext topologyContext,
            SpoutOutputCollector spoutOutputCollector) {
        // create the buffer to block tweets
        queue = new LinkedBlockingQueue<>(1000);

        // save the output collector for emitting tuples
        collector = spoutOutputCollector;


        // build the config with credentials for twitter 4j
        ConfigurationBuilder config =
                new ConfigurationBuilder()
                        .setOAuthConsumerKey(custkey)
                        .setOAuthConsumerSecret(custsecret)
                        .setOAuthAccessToken(accesstoken)
                        .setOAuthAccessTokenSecret(accesssecret);

        // create the twitter stream factory with the config
        TwitterStreamFactory fact =
                new TwitterStreamFactory(config.build());

        // get an instance of twitter stream
        twitterStream = fact.getInstance();

        // provide the handler for twitter stream
        twitterStream.addListener(new TweetListener());

        // Filter
        FilterQuery filter = new FilterQuery();
        String[] keywordsArray = {"#GOT"};
        filter.track(keywordsArray);
        twitterStream.filter(filter);
    }

    @Override
    public void nextTuple() {
        // try to pick a tweet from the buffer
        String ret = queue.poll();

        // if no tweet is available, wait for 50 ms and return
        if (ret == null) {
            Utils.sleep(50);
            return;
        }

        // now emit the tweet to next stage bolt
        collector.emit(new Values(ret));
    }

    @Override
    public void close() {
        // shutdown the stream - when we are going to exit
        twitterStream.shutdown();
    }

    /**
     * Component specific configuration
     */
    @Override
    public Map<String, Object> getComponentConfiguration() {
        // create the component config
        Config ret = new Config();

        // set the parallelism for this spout to be 1
        ret.setMaxTaskParallelism(1);

        return ret;
    }

    @Override
    public void declareOutputFields(
            OutputFieldsDeclarer outputFieldsDeclarer) {
        // tell storm the schema of the output tuple for this spout
        // tuple consists of a single column called 'tweet'
        outputFieldsDeclarer.declare(new Fields("tweet-user"));
//        outputFieldsDeclarer.declare(new Fields("tweet-user"));
    }

    // Class for listening on the tweet stream - for twitter4j
    private class TweetListener implements StatusListener {

        // Implement the callback function when a tweet arrives
        @Override
        public void onStatus(Status status) {
            // add the tweet into the queue buffer

            if (isValidTweet(status.getText())) {
                System.out.println("Twitter Msg: " + status.getText() + " User: " + status.getUser().getName());
                queue.offer(status.getUser().getName());
            }
        }

        private boolean isValidTweet(String text) {
            return text.contains("ftw");
        }

        @Override
        public void onDeletionNotice(StatusDeletionNotice sdn) {
        }

        @Override
        public void onTrackLimitationNotice(int i) {
        }

        @Override
        public void onScrubGeo(long l, long l1) {
        }

        @Override
        public void onStallWarning(StallWarning warning) {
        }

        @Override
        public void onException(Exception e) {
            e.printStackTrace();
        }
    }
}
