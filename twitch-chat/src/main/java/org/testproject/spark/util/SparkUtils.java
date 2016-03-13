package org.testproject.spark.util;

import com.google.common.base.Optional;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.testproject.spark.twitch.util.TwitchApiHelper.extractUser;

/**
 * Created by akurilyonok
 */
public class SparkUtils implements Serializable {

    public static final String SPACE = " ";
    public static final String PRIVMSG_PATTERN = "PRIVMSG #%s";

    public static final Function3<String, Optional<Long>, State<Long>, Tuple2<String,Long>> mappingDiffFunction = (s, longOptional, longState) ->{
        Long delta = longOptional.or(0l) - (longState.exists()? longState.get(): 0l);
        Tuple2<String, Long> updated = new Tuple2<>(s, delta);
        longState.update(delta);
        return updated;
    };


    public static final Function3<String, Optional<Long>, State<Long>, Tuple2<String,Long>> mappingSumFunction = (s, longOptional, longState) -> {
        Long sum = longOptional.or(0l) + (longState.exists()? longState.get(): 0l);
        Tuple2<String, Long> updated = new Tuple2<>(s, sum);
        longState.update(sum);
        return updated;
    };


    public static JavaPairDStream<String,Long> prepareEmoticons(List<String> emoticonList, JavaDStream<String> wLines) {
        JavaDStream<String> emoticons = wLines.flatMap(s -> Arrays.asList(s.split(SPACE))).filter(emoticonList::contains);
        return emoticons.countByValue();
    }

    public static JavaPairDStream<String,Long> prepareUserStatistics(String channel, JavaDStream<String> wLines) {
        JavaDStream<String> users = wLines.filter(s -> s.contains(String.format(PRIVMSG_PATTERN, channel))).flatMap(s -> Collections.singletonList(extractUser(s)));
        return users.countByValue();
    }


    public static JavaPairDStream<Long, String> calculateFunctionalState(JavaPairDStream<String, Long> preparedStatistics,
                                                                          Function3<String, Optional<Long>, State<Long>, Tuple2<String,Long>> func,
                                                                          JavaPairRDD<String, Long> initialRdd){
        JavaMapWithStateDStream<String, Long, Long, Tuple2<String,Long>> state =
                preparedStatistics.mapWithState(StateSpec.function(func).initialState(initialRdd));
        JavaPairDStream<Long, String> emoticonCountSwapped = state.mapToPair(Tuple2::swap);
        return emoticonCountSwapped.transformToPair(emoticonsAndCounts->emoticonsAndCounts.sortByKey(false));

    }



}
