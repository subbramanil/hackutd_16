package collabroscope;
/**
 * Created by Subbu on 2/12/16.
 */

import collabroscope.bolts.ParseTweetBolt;
import collabroscope.bolts.ReportBolt;
import collabroscope.spouts.TweetSpout;
import collabroscope.utils.CbConstants;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

class TweetTopology {
    public static void main(String[] args) throws Exception {
        // create the topology
        TopologyBuilder builder = new TopologyBuilder();

        // now create the tweet spout with the credentials
        TweetSpout tweetSpout = new TweetSpout(
                CbConstants.consumerKey,
                CbConstants.consumerSecret,
                CbConstants.token,
                CbConstants.secret
        );

        // attach the tweet spout to the topology - parallelism of 1
        builder.setSpout("tweet-spout", tweetSpout, 1);

        // attach the parse tweet bolt using shuffle grouping
        builder.setBolt("parse-tweet-bolt", new ParseTweetBolt(), 5).shuffleGrouping("tweet-spout");

        // attach the report bolt using global grouping - parallelism of 1
        builder.setBolt("report-bolt", new ReportBolt(), 1).globalGrouping("parse-tweet-bolt");

        // create the default config object
        Config conf = new Config();

        // set the config in debugging mode
        conf.setDebug(true);

        if (args != null && args.length > 0) {

            // run it in a live cluster

            // set the number of workers for running all spout and bolt tasks
            conf.setNumWorkers(3);

            // create the topology and submit with config
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());

        } else {

            // run it in a simulated local cluster

            // set the number of threads to run - similar to setting number of workers in live cluster
            conf.setMaxTaskParallelism(2);

            // create the local cluster instance
            LocalCluster cluster = new LocalCluster();

            // submit the topology to the local cluster
            cluster.submitTopology("tweet-word-count", conf, builder.createTopology());

            // let the topology run for 300 seconds. note topologies never terminate!
            Utils.sleep(300000);
/*
            // now kill the topology
            cluster.killTopology("tweet-word-count");

            // we are done, so shutdown the local cluster
            cluster.shutdown();
*/
        }
    }
}
