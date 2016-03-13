package org.testproject.spark.twitch.util;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;

/**
 *
 * Created by akurilyonok
 */
public class TwitchApiHelper {
    public static final String TWITCH_API_URL = "https://api.twitch.tv/kraken";
    public static final String TWITCH_API_EMOTICON_LIST_URI = "/chat/emoticons";
    public static final String TWITCH_STREAM_TOP_CHANNELS = "/streams?stream_type=all&limit=10";
    public static final String URL_PATTERN = "%s%s";
    public static final String P_CHAT_EMOTICONS = "emoticons";
    public static final String P_STREAMS = "streams";
    public static final String P_REGEX = "regex";
    public static final String P_STREAM_CHANNEL = "channel";
    public static final String P_CHANNEL_NAME = "name";

    public static String buildRequestUrl(String rootUrl, String requestUrl) {
        return String.format(URL_PATTERN, rootUrl, requestUrl);
    }

    /**
     * Requests Twitch public API and retrieves the list of all emoticons
     * @return list of emoticon regular expressions
     * @throws IOException
     */
    public static List<String> getEmoticonList() throws IOException{
        URL emoticonUrl = new URL(buildRequestUrl(TWITCH_API_URL,TWITCH_API_EMOTICON_LIST_URI));
        HttpURLConnection connection = (HttpURLConnection)emoticonUrl.openConnection();
        connection.setRequestMethod("GET");
        connection.setDoOutput(true);
        BufferedReader in = new BufferedReader(
                new InputStreamReader(connection.getInputStream()));
        String inputLine;
        StringBuilder response = new StringBuilder();

        while ((inputLine = in.readLine()) != null) {
            response.append(inputLine);
        }
        in.close();
        connection.disconnect();
        return JacksonUtils.asList(response.toString(), P_CHAT_EMOTICONS, P_REGEX);

    }

    /**
     * Retrieves the list of channels by TOP10 streams according to count of users
     * @return list of channel display names
     * @throws IOException
     */
    public static List<String> getChannelsByTopTenStreams() throws IOException{
        URL emoticonUrl = new URL(buildRequestUrl(TWITCH_API_URL,TWITCH_STREAM_TOP_CHANNELS));
        HttpURLConnection connection = (HttpURLConnection)emoticonUrl.openConnection();
        connection.setRequestMethod("GET");
        connection.setDoOutput(true);
        BufferedReader in = new BufferedReader(
                new InputStreamReader(connection.getInputStream()));
        String inputLine;
        StringBuilder response = new StringBuilder();
        while ((inputLine = in.readLine()) != null) {
            response.append(inputLine);
        }
        in.close();
        connection.disconnect();
        return JacksonUtils.asList(response.toString(), P_STREAMS, P_STREAM_CHANNEL, P_CHANNEL_NAME);
    }

    /**
     * Extracts username from the chat message
     * @param chatMessage - a single chat message received from Twitch
     * @return
     */
    public static  String extractUser(String chatMessage) {
        return chatMessage.substring(chatMessage.indexOf(":") + 1, chatMessage.indexOf("!"));
    }


}
