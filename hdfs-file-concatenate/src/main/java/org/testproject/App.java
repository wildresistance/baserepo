package org.testproject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.logging.Logger;

/**
 * Консольное приложение для задачи 2
 * Запускается через spark-submit либо mvn:exec
 *
 * @author Anton Kurilyonok
 */
public class App {
    private static final  Logger log = Logger.getLogger("App.class");
    private static final String A_FILE_PATTERN = "%d_a.log";
    private static final String B_FILE_PATTERN = "%d_b.log";
    private static final String C_FILE_PATTERN = "%d_c.log";
    private static final String FINAL_FILE_PATTERN = "%d_abc.log";
    private static final String PATH = "%s/%s/%s";


    public static void main(String[] args) throws Exception {
        // получение необходимых аргументов из командной строки
        if (args.length == 0) {
            log.info("Required parameters are not set");
            log.info("usage: java -cp <path to spark lib> org.testproject.App <path to hdfs> <file directory> <number of parts>");
            log.info("or mvn exec:java ");
            log.info("or use spark-submit to launch the application");
            System.exit(0);
        }

        String hdfsPath = args[0];
        if (null == hdfsPath) {
            log.info("path to hdfs is missing");
            System.exit(0);
        }

        String directory = args[1];
        if (null == directory) {
            log.info("'file directory' parameter is missing");
            System.exit(0);
        }
        Integer count = Integer.valueOf(args[2]);
        if (null == count) {
            log.info("'number of files' parameter is missing");
            System.exit(0);
        }
        //Инициализация конфигурации Spark. Для тестового задания используем количество потоков = 2 (local[2] режим)
        SparkConf conf = new SparkConf().setAppName("test").setMaster("local[2]");
        JavaSparkContext context = new JavaSparkContext(conf);
        for (int i = 1; i <= count; i++) {
            //repartition - указываем явно количество разделов для паралелизации = 2
            final JavaRDD<String> fileA = context.textFile(buildFilePath(A_FILE_PATTERN, hdfsPath, directory, i)).repartition(2);
            final JavaRDD<String> fileB = context.textFile(buildFilePath(B_FILE_PATTERN, hdfsPath, directory, i)).repartition(2);
            final JavaRDD<String> fileC = context.textFile(buildFilePath(C_FILE_PATTERN, hdfsPath, directory, i)).repartition(2);
            JavaRDD<String> zippedFile = zipFiles(fileA, fileB, fileC);

            zippedFile.saveAsTextFile(buildFilePath(FINAL_FILE_PATTERN, hdfsPath, directory, i));
        }

        context.stop();
    }

    protected static JavaRDD<String> zipFiles(JavaRDD<String> fileA, JavaRDD<String> fileB, JavaRDD<String> fileC) {
        final JavaPairRDD<String, String> fileAB = fileA.zip(fileB);
        final JavaRDD<String> abConcat = fileAB.map(v1 -> v1._1() + v1._2());
        final JavaPairRDD<String, String> fileABC = abConcat.zip(fileC);
        return fileABC.map(v1 -> v1._1() + v1._2());
    }

    protected static String buildFilePath(String pattern, String hdfsPath, String directory, int filePart) {
       return  String.format(PATH, hdfsPath, directory,
                String.format(pattern, filePart));
    }
}
