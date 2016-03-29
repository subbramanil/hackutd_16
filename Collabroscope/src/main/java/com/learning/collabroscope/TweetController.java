package collabroscope;

import collabroscope.utils.CbConstants;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.auth.AccessToken;

/**
 * Created by Subbu on 2/11/16.
 */
public class TweetController {
    public static void retweetMsg(Long tweetMsgID) {
        System.out.println("Retweet Msg: ID: " + tweetMsgID);
        TwitterFactory factory = new TwitterFactory();
        Twitter twitter = factory.getInstance();
        twitter.setOAuthConsumer(CbConstants.consumerKey, CbConstants.consumerSecret);
        AccessToken accessToken = new AccessToken(CbConstants.token, CbConstants.secret);
        twitter.setOAuthAccessToken(accessToken);
        try {
            twitter.retweetStatus(tweetMsgID);
        } catch (TwitterException e) {
            System.out.println("Status Code: " + e.getStatusCode());
            if (e.getStatusCode() == 403) {
                System.out.println(e.getErrorMessage());
            } else {
                e.printStackTrace();
            }
        }
    }
}
