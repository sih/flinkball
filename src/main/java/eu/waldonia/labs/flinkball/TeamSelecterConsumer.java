package eu.waldonia.labs.flinkball;

import org.apache.flink.api.java.DataSet;

/**
 * @author sih
 */
public class TeamSelecterConsumer {

    private ResultFileNormalizer normalizer;

    public TeamSelecterConsumer() {
        normalizer = new ResultFileNormalizer();
    }

    public void filter(final String teamNameFilter) throws Exception {
        String teamName = (null == teamNameFilter) ? "West Ham" : teamNameFilter;

        DataSet<FootyResult> results = normalizer.normalize();

        DataSet<FootyResult> filteredResults =
                results.filter(row -> row.getHomeTeam().equals(teamName) || row.getAwayTeam().equals(teamName));
        filteredResults.print();
    }

    public static void main(String[] args) {
        try {
            TeamSelecterConsumer consumer = new TeamSelecterConsumer();
            if (args.length == 0) consumer.filter(null);
            else consumer.filter(args[0]);
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

}
