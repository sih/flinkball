package eu.waldonia.labs.flinkball;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * Shows use of a map function to cleanse (normalize in this case) the data
 * @author sih
 */
public class ResultFileNormalizer {

    private static final String OUTPUT_FILE_PATH = "/Users/sid/dev/teleprinter/output/results.csv";

    /**
     * This takes the input files (which vary in format over the years) and creates a single, normalized format that
     * represents the 'lowest common denominator', i.e. the common fields that all files share. This is essentially the
     * data in the (base) FootyResult class.
     *
     * @throws Exception
     */
    public DataSet<FootyResult> normalize() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<FootyResult> normalizedResults = env
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
                    })
                ;

        return normalizedResults;

    }

    public static void main(String[] args) throws Exception {
        ResultFileNormalizer consumer = new ResultFileNormalizer();
        consumer.normalize();
    }


}
