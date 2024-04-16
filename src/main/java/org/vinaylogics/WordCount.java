package org.vinaylogics;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;

public class WordCount {

    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        ParameterTool params = ParameterTool.fromArgs(args);

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // read the text file from given inout path
        DataSet<String> text = env.readTextFile(params.get("input")); // text = [Noman Joyce Noman Sayuri Frances...]

        // Filter all the names starts with N
        DataSet<String> filtered = text.filter((FilterFunction<String>) value -> value.startsWith("N"));
        //filtered = dataset of [Noman Nipun Noman]

        // Returns a tuple of (name, 1)
        DataSet<Tuple2<String, Integer>> tokenized = filtered.map(new Tokenizer()); // tokenized = [(Noman, 1), (Nipun, 1), (Noman, 1)]

        // group by tuple field "0" and sum up tuple field "1"
        DataSet<Tuple2<String, Integer>> counts = tokenized.groupBy(0).sum(1);
        if (params.has("output"))
        {
            counts.writeAsCsv(params.get("output"), "\n", " ");

            env.execute("WordCount Example");
        }

    }
}
