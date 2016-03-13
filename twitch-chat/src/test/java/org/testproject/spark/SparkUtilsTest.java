package org.testproject.spark;

import junit.framework.Assert;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.testproject.spark.util.SparkUtils;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

import static org.testproject.spark.util.SparkUtils.*;
import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

/**
 * Created by macbook on 06.03.16.
 */

public class SparkUtilsTest implements Serializable {
    private static final String TEST_MASTER = "local[2]";
    private static final String APP_NAME = "twtich-chat-test";
    private static final String CHANNEL_NAME = "rocketbeanstv";


    private final List<String> emoticonList = Arrays.asList(":)", ":D", ";)");
    private final List<String> userList = Arrays.asList("godtone987", "gamrgrll", "cleaner4u",
            "q119311", "steinuh", "ac9980192", "vlad7613", "eberx_", "pagar20");

    private transient JavaStreamingContext jsc;

    @Before
    public void setUp() {
        SparkConf conf = new SparkConf()
                .setAppName(APP_NAME)
                .setMaster(TEST_MASTER);
        jsc = new JavaStreamingContext(conf, Durations.seconds(2));
    }



    @Test
    public void testPreparedEmoticons() {

        String testInput = "Hi! :) This is test input for unit test :) :) :) :D Hope you enjoy it ;)";
        List<String> inputs = Collections.singletonList(testInput);
        JavaRDD<String> testInputRdd = jsc.sparkContext().parallelize(inputs);
        List<Tuple2<String, Long>> expectedResult = Arrays.asList(new Tuple2<>(":)", 4l), new Tuple2<>(":D", 1l), new Tuple2<>(";)", 1l));
        List<Tuple2<String, Long>> firstResult = Collections.singletonList(new Tuple2<>(":)", 4l));
        JavaDStream<String> stream = jsc.queueStream(new PriorityQueue<>(Collections.singletonList(testInputRdd)), true);
        JavaPairDStream<String,Long> pairDStream = prepareEmoticons(emoticonList, stream);
        pairDStream.foreachRDD((s) -> {
            Assert.assertEquals(s.count(), (long) emoticonList.size());
            List<Tuple2<String, Long>> result = s.take(3);
            assertThat(result, is(expectedResult));
            s.sortByKey(false);
            List<Tuple2<String, Long>> first = s.take(1);
            assertEquals(first, firstResult);
        });
        jsc.start();

    }
    @Test
    public void testPreparedUsers() {
        JavaDStream<String> stream = jsc.textFileStream("resources/log_2.txt");
        JavaPairDStream<String,Long> pairDStream = prepareUserStatistics(CHANNEL_NAME, stream);
        List<Tuple2<String, Long>> expectedResult = Arrays.asList(new Tuple2<>("godtone987", 1l), new Tuple2<>("gamrgrll", 1l), new Tuple2<>("cleaner4u", 1l),
                new Tuple2<>("q119311", 1l), new Tuple2<>("steinuh", 1l), new Tuple2<>("ac9980192", 1l),
                new Tuple2<>("vlad7613", 1l), new Tuple2<>("eberx_", 1l), new Tuple2<>("pagar20", 2l));
        List<Tuple2<String, Long>> firstResult = Collections.singletonList(new Tuple2<>("pagar20", 2l));
        pairDStream.foreachRDD((s) -> {
            Assert.assertEquals(s.count(), (long) userList.size());
            List<Tuple2<String, Long>> result = s.take(9);
            assertThat(result, is(expectedResult));
            s.sortByKey(false);
            List<Tuple2<String, Long>> first = s.take(1);
            assertEquals(first, firstResult);
        });
        jsc.start();
    }

    @Test
    public void testCalculateFunctionalState() {
        jsc.checkpoint(".");
        String testInput = "Hi! :) This is test input for unit test :) :) :) :D Hope you enjoy it ;)";
        List<String> inputs = Collections.singletonList(testInput);
        JavaRDD<String> testInputRdd = jsc.sparkContext().parallelize(inputs);
        List<Tuple2<String, Long>> tupleList = Collections.singletonList(new Tuple2<>(":)", 4l));
        JavaPairRDD<String, Long> initial = jsc.sparkContext().parallelizePairs(tupleList);
        List<Tuple2<Long, String>> expectedResult = Arrays.asList(new Tuple2<>(8l,":)"), new Tuple2<>(1l,":D"), new Tuple2<>(1l,";)"));
        List<Tuple2<Long, String>> firstResult = Collections.singletonList(new Tuple2<>(8l, ":)"));
        JavaDStream<String> stream = jsc.queueStream(new PriorityQueue<>(Collections.singletonList(testInputRdd)), true);
        JavaPairDStream<String,Long> pairDStream = prepareEmoticons(emoticonList, stream);
        JavaPairDStream<Long, String> p = calculateFunctionalState(pairDStream, mappingSumFunction, initial);
        p.foreachRDD((s) -> {
            Assert.assertEquals(s.count(), (long) emoticonList.size());
            List<Tuple2<Long, String>> result = s.take(3);
            assertThat(result, is(expectedResult));
            s.sortByKey(false);
            List<Tuple2<Long, String>> first = s.take(1);
            assertEquals(first, firstResult);
        });
        jsc.start();
    }

@Before
    public void tearDown() {
    if (null != jsc)  {
        jsc.stop();
        jsc.close();
    }
}


}
