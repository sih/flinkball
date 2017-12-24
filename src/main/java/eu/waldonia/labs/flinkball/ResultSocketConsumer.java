package eu.waldonia.labs.flinkball;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * @author sih
 */
public class ResultSocketConsumer {

    private String host;
    private int port;
    private String delimiter;

    /**
     * Lazy constructor to accept all defaults
     */
    public ResultSocketConsumer() {
        this.host = DEFAULT_HOST;
        this.port = DEFAULT_PORT;
        this.delimiter= DEFAULT_DELIMITER;
    }

    private static final String DEFAULT_HOST = "localhost";
    private static final int DEFAULT_PORT = 33333;
    private static final String DEFAULT_DELIMITER = "\n";

    public void streamResults() throws Exception {
        // https://ci.apache.org/projects/flink/flink-docs-release-1.3/dev/local_execution.html#local-environment
        // ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<FootyResult> resultsStream =
                env
                .socketTextStream(host, port, delimiter)
                .map(new MapFunction<String, FootyResult>() {
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

        resultsStream.print();
        env.execute("Results Stream");

    }


    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    public static void main(String[] args) throws Exception {
        ResultSocketConsumer consumer = new ResultSocketConsumer();
        consumer.streamResults();
    }


}
