package org.testproject.spark.receiver;

import org.apache.log4j.Logger;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

import java.io.*;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.List;

/**
 * Created by akurilyonok
 */
public class TwitchReceiver  extends Receiver<String>{
    private static final String ANONYMOUS_USER = "justinfan12340905";
    private static final Logger logger = Logger.getLogger(TwitchReceiver.class);

    private final String host;
    private final int port;
    private final List<String> channels;

    public TwitchReceiver(String host, int port, List<String> channels) {
        super(StorageLevel.MEMORY_AND_DISK_2());
        this.host = host;
        this.port = port;
        this.channels = channels;
    }

    @Override
    public void onStart() {
        new Thread() {
            public void run() {
               establishConnection();
            }
        }.start();

    }

    @Override
    public void onStop() {

    }

    private void establishConnection() {
        try {
            InetSocketAddress inetSocketAddress = new InetSocketAddress(host, port);
            Socket socket = null;
            BufferedReader reader = null;
            BufferedWriter writer;
            String userInput;
            try {
                // connect to the server
                socket = new Socket();
                socket.connect(inetSocketAddress);
                reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));


                writer.write(String.format("NICK %s\r\n", ANONYMOUS_USER));
                for (String channel: channels) {
                    writer.write(String.format("JOIN #%s \r\n", channel));
                    writer.flush();
                }
                logger.info("Successfully connected to twitch irc");
                while (!isStopped() && (userInput = reader.readLine()) != null) {
                    if (logger.isDebugEnabled()) {
                        logger.debug(userInput);
                    }
                    store(userInput);
                }

            } finally {
                if (null != reader) {
                    try {
                        reader.close();
                    } catch (IOException ex) {
                        logger.error("An error has occured while closing reader", ex);
                    }
                }

                if (null != socket) {
                    try {
                        socket.close();
                    } catch (IOException ex) {
                        logger.error("An error has occured while closing socket", ex);
                    }
                }
            }
            restart("Trying to connect again");
        } catch(ConnectException ce) {
            restart("Could not connect", ce);
        } catch(Throwable t) {
            restart("Error receiving data", t);
        }
    }

}
