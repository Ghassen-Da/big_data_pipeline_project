package streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;

import config.DatabaseConfig;
import scala.Tuple2;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

public class SalaryStream {
    private static final Pattern SPACE = Pattern.compile(" ");

    private SalaryStream() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: SparkKafkaWordCount <zkQuorum> <group> <topics> <numThreads>");
            System.exit(1);
        }

        SparkConf sparkConf = new SparkConf().setAppName("SparkKafkaWordCount");
        // Creer le contexte avec une taille de batch de 2 secondes
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf,
            new Duration(2000));

        int numThreads = Integer.parseInt(args[3]);
        Map<String, Integer> topicMap = new HashMap<>();
        String[] topics = args[2].split(",");
        for (String topic: topics) {
            topicMap.put(topic, numThreads);
        }

        JavaPairReceiverInputDStream<String, String> messages =
                KafkaUtils.createStream(jssc, args[0], args[1], topicMap);
        

        JavaDStream<String> lines = messages.map(Tuple2::_2);

        JavaDStream<List> words =
                lines.map(x -> Arrays.asList(SPACE.split(x)));

        JavaPairDStream<String, Integer> output =
                words.mapToPair(s -> {
                	DatabaseConfig.main(s.get(0).toString(),s.get(1).toString());
                	return new Tuple2<>(s.get(0).toString(), Integer.parseInt((String) s.get(1)));
                });
        
        
        
        words.print();
        jssc.start();
        
        
        
        jssc.awaitTermination();
    }
    
}
