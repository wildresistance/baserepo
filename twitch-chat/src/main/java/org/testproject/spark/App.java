package org.testproject.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.testproject.spark.receiver.TwitchReceiver;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.testproject.spark.twitch.util.TwitchApiUtils.*;
import static org.testproject.spark.util.SparkUtils.*;

/**
 * Created by akurilyonok
 */
public class App {

    public static void main(String args[]) {
        if (args.length < 4) {
            throw new IllegalStateException("Not all the parameters have been set");
        }
        String masterPath = args[0];
        String twitchHost = args[1];
        Integer twitchPort = Integer.parseInt(args[2]);
        Integer topCount = Integer.parseInt(args[3]);
        List<String> emoticonList = getEmoticonList();
        List<String> topChannelLists = getChannelsByTopTenStreams();

        SparkConf sparkConf = new SparkConf().setMaster(masterPath).setAppName("twitch-chat");

        JavaStreamingContext jsc = new JavaStreamingContext(sparkConf, Durations.seconds(2));
        jsc.checkpoint(".");
        JavaReceiverInputDStream<String> lines = jsc.receiverStream(new TwitchReceiver(twitchHost, twitchPort, topChannelLists));
        JavaDStream<String> wLines = lines.window(Durations.minutes(5), Durations.minutes(5));

        List<Tuple2<String, Long>> tupleList = new ArrayList<>();
        JavaPairRDD<String, Long> initial = jsc.sparkContext().parallelizePairs(tupleList);

        JavaPairDStream<String, Long> preparedStatistics = prepareEmoticons(emoticonList, wLines);

        JavaPairDStream<Long, String> topEmoticons= calculateFunctionalState(preparedStatistics, mappingSumFunction, initial);

        printResult(topEmoticons, topCount, "\n Top emoticons");

        JavaPairDStream<Long, String> top10RisingEmoticons = calculateFunctionalState(preparedStatistics, mappingDiffFunction, initial);
        printResult(top10RisingEmoticons, topCount, "\n Top rising emoticons");

        topChannelLists.stream().forEach((channel) -> {
            JavaPairDStream<String, Long> preparedUserStatistics = prepareUserStatistics(channel, wLines);
            JavaPairDStream<Long, String> state = calculateFunctionalState(preparedUserStatistics, mappingSumFunction, initial);
            printResult(state, topCount, String.format("\n Top users for channel %s", channel));
        });

        jsc.start();
        jsc.awaitTermination();

    }

    private static void printResult(JavaPairDStream<Long, String> stream, int topCount, String headerMessage) {
        stream.foreachRDD((streamRdd, time)->{
            System.out.println(headerMessage);
            System.out.println(new Date(time.milliseconds()));
            streamRdd.take(topCount).stream().forEach((pair)-> System.out.println(String.format("%s (%s occurences)", pair._2(), pair._1())));
        });
    }


}
