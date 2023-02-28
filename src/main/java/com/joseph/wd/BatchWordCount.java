package com.joseph.wd;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


public class BatchWordCount {

    public static void main(String[] args) throws Exception{
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> lineDS = executionEnvironment.readTextFile("input/words.txt");

        FlatMapOperator<String, Tuple2<String, Long>> wordAndOneTuple = lineDS.flatMap(
                (String line, Collector<Tuple2<String, Long>> out) ->
                {
                    String[] words = line.split(" ");
                    //将每个单词转换成二元组
                    for (String word : words) {
                        out.collect(Tuple2.of(word, 1L));
                    }
                }
        ).returns(Types.TUPLE(Types.STRING, Types.LONG));
        AggregateOperator<Tuple2<String, Long>> aggregate = wordAndOneTuple.groupBy(0).sum(1);
        aggregate.print();
    }
}
