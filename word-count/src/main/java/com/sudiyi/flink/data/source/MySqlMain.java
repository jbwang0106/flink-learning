package com.sudiyi.flink.data.source;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author sdy
 */
public class MySqlMain {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        environment.addSource(new SourceFromMySql()).print();
        environment.execute("Flink add data source");
    }
}
