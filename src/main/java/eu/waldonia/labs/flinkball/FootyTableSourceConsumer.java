package eu.waldonia.labs.flinkball;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.table.sources.TableSource;

/**
 * @author sih
 */
public class FootyTableSourceConsumer {

    private static final String OUTPUT_FILE_PATH = "/Users/sid/dev/teleprinter/output/results.csv";

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

    private String[] fieldNames;
    private TypeInformation[] fieldTypes;
    private static final String[] FIELD_NAMES = {
            "division","date","homeTeam","awayTeam","fullTimeHomeGoals","fullTimeAwayGoals","fullTimeResult"
    };

    public FootyTableSourceConsumer() {
        fieldNames = FIELD_NAMES;
        fieldTypes = new TypeInformation[7];
        fieldTypes[0] = TypeInformation.of(String.class);
        fieldTypes[1] = TypeInformation.of(String.class);
        fieldTypes[2] = TypeInformation.of(String.class);
        fieldTypes[3] = TypeInformation.of(String.class);
        fieldTypes[4] = TypeInformation.of(Integer.class);
        fieldTypes[5] = TypeInformation.of(Integer.class);
        fieldTypes[6] = TypeInformation.of(String.class);

    }



    public void consume() {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // get a StreamTableEnvironment, works for BatchTableEnvironment equivalently
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

        // create a TableSource
        TableSource csvSource = new CsvTableSource(OUTPUT_FILE_PATH, fieldNames, fieldTypes);

        // register the TableSource as table "ResultsTable"
        tableEnv.registerTableSource("ResultsTable", csvSource);

        Table results = tableEnv.scan("ResultsTable");

        Table hammers = results.filter("homeTeam === 'West Ham'");

        Table gols =
                results
                .groupBy("homeTeam")
                .select("fullTimeHomeGoals.sum AS gol");


        TableSink sink = new CsvTableSink("/Users/sid/dev/flinkball/output/hammers.csv", ",");
        hammers.writeToSink(sink);


    }


    public static void main(String[] args) {
        FootyTableSourceConsumer tableSourceConsumer = new FootyTableSourceConsumer();
        tableSourceConsumer.consume();
    }




}
