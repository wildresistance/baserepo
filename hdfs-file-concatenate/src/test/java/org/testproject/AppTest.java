package org.testproject;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 *
 */
public class AppTest {
    private SparkConf conf;
    private LocalFileSystem localFileSystem;
    private JavaSparkContext context;

    @Before
    public void setUp() throws Exception{
        conf = new SparkConf().setMaster("local[2]").setAppName("test");
        context = new JavaSparkContext(conf);
        localFileSystem = FileSystem.getLocal(new Configuration());
        Path path = new Path("src/test/resources/1_abc.log");
        localFileSystem.delete(path, Boolean.TRUE);
    }
    @After
    public void tearDown() throws Exception {
        context.close();
    }
    @Test
    public void testZipRdds() {
        List<String> fileA = Arrays.asList("a", "b", "c");
        List<String> fileB = Arrays.asList("d", "e", "f");
        List<String> fileC = Arrays.asList("g", "h", "i");
        List<String> expected = Arrays.asList("adg", "beh", "cfi");
        JavaRDD<String> finalFile = App.zipFiles(context.parallelize(fileA), context.parallelize(fileB), context.parallelize(fileC));
        List<String> result = finalFile.collect();
        assertEquals(result.get(0), expected.get(0));
        assertEquals(result.get(1), expected.get(1));
        assertEquals(result.get(2), expected.get(2));
    }

    @Test
    public void testZipFiles1() throws Exception {
        JavaRDD<String> finalFile = App.zipFiles(context.textFile("src/test/resources/1_a.log").repartition(2),
                context.textFile("src/test/resources/1_b.log").repartition(2), context.textFile("src/test/resources/1_c.log").repartition(2));
        finalFile.saveAsTextFile("src/test/resources/1_abc.log");
        Path path = new Path("src/test/resources/1_abc.log");
        assertTrue(localFileSystem.exists(path));
        JavaRDD<String> file = context.textFile("src/test/resources/1_abc.log");
        List<String> list = file.collect();
        list.forEach(System.out::println);
        JavaRDD<String> source = context.textFile("src/test/resources/source.log");
        List<String> sourceList = source.collect();
        assertEquals(list.size(), sourceList.size());
        list.removeAll(sourceList);
        assertEquals(list.size(), 0);
    }
}
