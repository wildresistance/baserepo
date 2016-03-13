#  Twitch chat client

> Implements a Twitch Chat client application running on Apache Spark (latest).
Application connects to the chat of top 10 channels(according to Twitch public API) with the highest number of viewers at the start of the application.
Application processes chat messages and extract the following information:
* "Top10" and "Rising10" used emoticons in all chats over the last 5 minutes.
* "Top10" active users by the channel with the count of messages over the last 5 minutes.

# How to run the application

** $ ./spark-submit --class org.testproject.spark.App 
                                /users/macbook/apps/twitch-chat-1.0-SNAPSHOT.jar 
                                irc.twitch.tv 6667

