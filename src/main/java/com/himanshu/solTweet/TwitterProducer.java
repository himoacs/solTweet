package com.himanshu.solTweet;

import com.google.common.collect.Lists;
import com.solacesystems.jms.SolConnectionFactory;
import com.solacesystems.jms.SolJmsUtility;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.yaml.snakeyaml.Yaml;

import javax.jms.*;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    public TwitterProducer(){}

    public void run() throws Exception {

        // load broker properties file
        Yaml yamlBroker = new Yaml();
        InputStream inputStreamBroker = yamlBroker.getClass().getClassLoader().getResourceAsStream("broker.yaml");
        Map<String, Object> brokerConfig = yamlBroker.load(inputStreamBroker);

        // Create Solace PubSub+ connection
        Connection connection =  createSolaceConnection(
                (String) brokerConfig.get("host"),
                (String) brokerConfig.get("vpn"),
                (String) brokerConfig.get("user"),
                (String) brokerConfig.get("pwd"));

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);


        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

        Client client = TwitterClient(msgQueue);

        // Establish a connection.
        client.connect();

        JSONParser parser = new JSONParser();

        // Poll for tweets and publish them to PubSub+
        while (!client.isDone()) {
            String msg = msgQueue.poll(5, TimeUnit.SECONDS);

            if(msg!=null){
                JSONObject jsonMsg = (JSONObject) parser.parse(msg);
                String lang = (String) jsonMsg.get("lang");
                publishMessageToSolace(session, msg, "tweets/v1/covid/" + lang);

                // Control the msg publish rate
                Thread.sleep(1000);
            }
        }

    }

    public Connection createSolaceConnection(String host, String vpn, String user, String pwd) throws Exception {

        SolConnectionFactory connectionFactory = SolJmsUtility.createConnectionFactory();
        connectionFactory.setHost(host);
        connectionFactory.setVPN(vpn);
        connectionFactory.setUsername(user);
        connectionFactory.setPassword(pwd);

        Connection connection = connectionFactory.createConnection();

        return connection;

    }

    public void publishMessageToSolace(Session session, String msg, String myTopic) throws JMSException {

        // Generate topic to publish tweet to
        Topic topic = session.createTopic(myTopic);

        String textMessage = msg;

        System.out.println("=======================================================");
        System.out.println("Publishing to topic: " + topic);
        System.out.println("Data: " + textMessage);
        System.out.println("=======================================================");

        // Create a ByteMessage
        BytesMessage message = session.createBytesMessage();
        message.writeBytes(textMessage.getBytes());

        // Publish message to the topic
        MessageProducer messageProducer = session.createProducer(topic);
        messageProducer.send(topic, message, DeliveryMode.NON_PERSISTENT,
                message.DEFAULT_PRIORITY, message.DEFAULT_TIME_TO_LIVE);
    }

    // load twitter properties file
    Yaml yamlTwitter = new Yaml();
    InputStream inputStreamTwitter = yamlTwitter.getClass().getClassLoader().getResourceAsStream("twitter.yaml");
    Map<String, Object> twitterConfig = yamlTwitter.load(inputStreamTwitter);


    public Client TwitterClient(BlockingQueue<String> msgQueue){

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        List<String> terms = Lists.newArrayList("covid-19", "coronavirus");
        hosebirdEndpoint.trackTerms(terms);

        Authentication hosebirdAuth = new OAuth1(
                (String) twitterConfig.get("consumerKey"),
                (String) twitterConfig.get("consumerSecret"),
                (String) twitterConfig.get("token"),
                (String) twitterConfig.get("tokenSecret")
                );


        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();
        return hosebirdClient;

    }

    public static void main(String[] args) throws Exception {
        new TwitterProducer().run();
    }

}
