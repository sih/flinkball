package eu.waldonia.labs.flinkball;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.common.functions.MapFunction;
/**
 * @author sih
 */
public class ResultFileConsumer {

    private static final String OUTPUT_FILE_PATH = "../teleprinter/output/results.csv";

    public void readFileResults() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<FootyResult> resultsFile =
                env
                .readTextFile(OUTPUT_FILE_PATH)
                .map(new MapFunction<String,FootyResult>() {
                    //Div,Date,HomeTeam,AwayTeam,FTHG,FTAG,FTR,
                    private String[] tokens;

                    @Override
                    public FootyResult map(String value) throws Exception {
                        tokens = value.split(",");
                        String division = tokens[0];
                        String date = tokens[1];
                        String homeTeam = tokens[2];
                        String awayTeam = tokens[3];
                        int fullTimeHomeTeamGoals = Integer.valueOf(tokens[4]);
                        int fullTimeAwayTeamGoals = Integer.valueOf(tokens[5]);
                        String fullTimeResult = tokens[6];

                        return new FootyResult(
                                division,
                                date,
                                homeTeam,
                                awayTeam,
                                fullTimeHomeTeamGoals,
                                fullTimeAwayTeamGoals,
                                fullTimeResult);
                    }

        });
        resultsFile.print();
        env.execute("Results File");

    }

    public static void main(String[] args) throws Exception {
        ResultFileConsumer consumer = new ResultFileConsumer();
        consumer.readFileResults();
    }


}
