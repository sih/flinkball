package eu.waldonia.labs.flinkball;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StreamTableEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.table.sources.TableSource;

/**
 * @author sih
 */
public class FootyTableSourceConsumer {

    private static final String OUTPUT_FILE_PATH = "/Users/sid/dev/teleprinter/output/results.csv";
    private static final String SINK_FILE_PATH = "/Users/sid/dev/flinkball/output/hammers.csv";

    /*
        This is what a FootyResult looks like:
        private String division;
        private String date;
        private String homeTeam;
        private String awayTeam;
        private int fullTimeHomeGoals;
        private int fullTimeAwayGoals;
        private String fullTimeResult;
    */


    public FootyTableSourceConsumer() {
    }

    /**
     *
     * @throws Exception
     */
    public void consume() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

        // create a TableSource
        TableSource csvSource = CsvTableSource.builder()
                .path(OUTPUT_FILE_PATH)
                .field("division", Types.STRING)
                .field("date", Types.STRING)
                .field("homeTeam", Types.STRING)
                .field("awayTeam", Types.STRING)
                .field("fullTimeHomeGoals", Types.INT)
                .field("fullTimeAwayGoals", Types.INT)
                .field("fullTimeResult", Types.STRING)
                .lineDelimiter("\n")
                .build()
                ;


        // register the TableSource as table "results"
        tableEnv.registerTableSource("results", csvSource);

        Table results = tableEnv.scan("results");

        /*
            NOTE: Understand how the TableAPI works ...
            The hammers Dynamic Table is effectively a CQ.
            This means that with an operation like group by/count the result needs to be updated as the data is read
            in.
            But a CSV Sink is an AppendStreamTableSink (i.e. you don't go back and rewrite rows) which means that you
            can't run this type of query with that type of Sink, otherwise you get:
            org.apache.flink.table.api.TableException: AppendStreamTableSink requires that Table has only insert changes.
         */
//        Table hammers = results
//                .filter("homeTeam === 'West Ham' || awayTeam === 'West Ham'")
//                .groupBy("fullTimeResult")
//                .select("fullTimeResult, fullTimeResult.count");
//
//        CsvTableSink sink = new CsvTableSink(SINK_FILE_PATH, ",");
//         hammers.writeToSink(sink);

        Table hammers = results
                .filter("homeTeam === 'West Ham' || awayTeam === 'West Ham'")
//                .groupBy("fullTimeResult") // should be ok without the group by as this can just append
                .select("date,fullTimeResult");

        CsvTableSink sink = new CsvTableSink(SINK_FILE_PATH, ",", 1, FileSystem.WriteMode.OVERWRITE);
        hammers.writeToSink(sink);

        tableEnv.execEnv().execute();


    }




    public static void main(String[] args) {
        try {
            FootyTableSourceConsumer tableSourceConsumer = new FootyTableSourceConsumer();
            tableSourceConsumer.consume();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }




}
