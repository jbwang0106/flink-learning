package org.wiki.edits;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;

/**
 * @author sdy
 */
public class WikipediaAnalysis {

    public static void main(String[] args) throws Exception {

        // set env
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        // add source
        DataStream<WikipediaEditEvent> dataStream = environment.addSource(new WikipediaEditsSource());

        // add transformation
        SingleOutputStreamOperator<Tuple2<String, Long>> result = dataStream.keyBy((KeySelector<WikipediaEditEvent, String>) WikipediaEditEvent::getUser)
                .timeWindow(Time.seconds(5))
                .fold(new Tuple2<>("", 0L), new FoldFunction<WikipediaEditEvent, Tuple2<String, Long>>() {
                    private static final long serialVersionUID = -1021283448387478793L;

            @Override
            public Tuple2<String, Long> fold(Tuple2<String, Long> acc, WikipediaEditEvent event) {
                acc.f0 = event.getUser();
                acc.f1 += event.getByteDiff();
                return acc;
            }
        });

        // add sink
        result.print();

        // execute
        environment.execute("flink for wiki");

    }
}
