package eu.waldonia.labs.flinkball;

import org.apache.flink.api.java.DataSet;

/**
 * Demonstrates use of filtering
 * @author sih
 */
public class TeamSelecterConsumer {

    private ResultFileNormalizer normalizer;

    public TeamSelecterConsumer() {
        normalizer = new ResultFileNormalizer();
    }

    public DataSet<FootyResult> filter(final String teamNameFilter) throws Exception {
        String teamName = (null == teamNameFilter) ? "West Ham" : teamNameFilter;

        DataSet<FootyResult> results = normalizer.normalize();

        DataSet<FootyResult> filteredResults =
                results.filter(row -> row.getHomeTeam().equals(teamName) || row.getAwayTeam().equals(teamName));

        return filteredResults;
    }

    public static void main(String[] args) {
        try {
            TeamSelecterConsumer consumer = new TeamSelecterConsumer();
            DataSet<FootyResult> filteredResults = (args.length == 0) ? consumer.filter(null) : consumer.filter(args[0]);
            filteredResults.print();

        } catch (Exception e) {
            e.printStackTrace();
        }


    }

}
