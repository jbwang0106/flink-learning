package com.sudiyi.flink.transformation;

import com.sudiyi.flink.data.model.Student;
import com.sudiyi.flink.data.source.SourceFromMySql;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author sdy
 */
public class TransformationMain {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Student> dataStreamSource = environment.addSource(new SourceFromMySql());

        // test map
//        dataStreamSource.map((MapFunction<Student, Student>) value -> {
//            Student student = new Student();
//            student.setId(value.getId());
//            student.setName(value.getName());
//            student.setPassword(value.getPassword());
//            student.setAge(value.getAge() + 100);
//            return student;
//        }).print();

        // test flatMap
//        dataStreamSource.flatMap(new FlatMapFunction<Student, Student>() {
//            @Override
//            public void flatMap(Student value, Collector<Student> out) {
//                if (value.getId() % 2 == 0) {
//                    out.collect(value);
//                }
//            }
//        }).print();

        // test fliter
//        dataStreamSource.filter((FilterFunction<Student>) value -> {
//            if (value.getId() > 90) {
//                return true;
//            }
//            return false;
//        }).print();

        // test keyBy， keyBy在逻辑上是基于key对流进行分区。在内部它使用hash函数对流进行分区。它返回keyedDataStream数据流
        dataStreamSource.keyBy((KeySelector<Student, Integer>) Student::getAge).print();

        environment.execute("flink test transformation");

    }
}
