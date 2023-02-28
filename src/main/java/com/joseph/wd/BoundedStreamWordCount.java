package com.joseph.wd;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class BoundedStreamWordCount {
    public static void main(String[] args) throws Exception {
        //流式
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataSource = executionEnvironment.readTextFile("input/words.txt");
        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOne = dataSource.flatMap(
                (String line, Collector<Tuple2<String, Long>> out) ->
                {
                    String[] words = line.split(" ");
                    //将每个单词转换成二元组
                    for (String word : words) {
                        out.collect(Tuple2.of(word, 1L));
                    }
                }
        ).returns(Types.TUPLE(Types.STRING, Types.LONG));
        KeyedStream<Tuple2<String, Long>, String> tuple2StringKeyedStream = wordAndOne.keyBy(data -> data.f0);
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = tuple2StringKeyedStream.sum(1);
        sum.print();
        executionEnvironment.execute();
    }
}
