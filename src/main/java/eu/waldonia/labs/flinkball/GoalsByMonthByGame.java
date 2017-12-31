package eu.waldonia.labs.flinkball;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * Demonstrates joining two datasets including how to do a multi-key join
 * @author sih
 */
public class GoalsByMonthByGame {

    // Note that can't reuse this here as when we join we will get an error that the two inputs have different execution contexts
//    private GoalsByMonth goalsByMonth;
    private ResultFileNormalizer normalizer;

    public GoalsByMonthByGame() {
        normalizer = new ResultFileNormalizer();
    }

    /**
     * This demos a multi-key join between two datasets: goalsPerMonth (per Division) and gamesPerMonth (perDivision)
     * @return A tuple containing the month, division, and goals per game
     * @throws Exception
     */
    public DataSet<Tuple3<Integer, String, Double>> goalsPerGamePerMonth() throws Exception {

        // clean up the data
        DataSet<FootyResult> results = normalizer.normalize();

        /*
            Get the goals per month as per the output in the GoalsByMonth class
         */
        DataSet<Tuple3<Integer,String,Integer>> golsPerMonth =
                results.map(new MapFunction<FootyResult, Tuple3<Integer, String, Integer>>() {
                    @Override
                    public Tuple3<Integer, String, Integer> map(FootyResult footyResult) throws Exception {
                        return new Tuple3<>(
                                footyResult.month(),
                                footyResult.getDivision(),
                                footyResult.getFullTimeAwayGoals()+footyResult.getFullTimeHomeGoals());
                    }
                })
                        .groupBy(0,1)   // group by month and division
                        .sum(2)         // sum the number of goals
                ;

        /*
            Get the games per month thgouth very similar logic
         */
        DataSet<Tuple3<Integer,String,Integer>> gamesPerMonth =
                results.map(new MapFunction<FootyResult, Tuple3<Integer, String, Integer>>() {
                    @Override
                    public Tuple3<Integer, String, Integer> map(FootyResult footyResult) throws Exception {
                        return new Tuple3<>(
                                footyResult.month(),
                                footyResult.getDivision(),
                                1);     // every result represents a game
                    }
                })
                        .groupBy(0,1)   // group by month and division
                        .sum(2)         // add the number of games
                ;

        /*
            Join these two datasets on month and division then divide the goals by the games to give the average goals
            per game per month per division - a more nuanced result than just the goals per month
         */
        DataSet<Tuple3<Integer, String, Double>> goalsPerGamePerMatch =
                golsPerMonth.join(gamesPerMonth)
                        .where(0,1).equalTo(0,1)    // join the two datasets by month and division
                .map(new MapFunction<Tuple2<Tuple3<Integer, String, Integer>, Tuple3<Integer, String, Integer>>, Tuple3<Integer, String, Double>>() {
                    @Override
                    public Tuple3<Integer, String, Double> map(Tuple2<Tuple3<Integer, String, Integer>, Tuple3<Integer, String, Integer>> joinedDataset) throws Exception {
                        Integer month = joinedDataset.f0.f0;    // get the month from the first dataset
                        String division = joinedDataset.f0.f1;  // get the division from the first dataset

                        // divide the goals per month (f0.f1) by the matches per month (f1.f1)
                        Double goalsPerGamePerMonth = (joinedDataset.f0.f2.doubleValue()/joinedDataset.f1.f2.doubleValue());
                        Double roundedGoalsPerGamePerMonth = Math.round(goalsPerGamePerMonth * 100D)/100D;

                        return new Tuple3<>(month,division,roundedGoalsPerGamePerMonth);
                    }
                });

        return goalsPerGamePerMatch;

    }

    public static void main(String[] args) {
        try {
            GoalsByMonthByGame gols = new GoalsByMonthByGame();
            DataSet<Tuple3<Integer,String, Double>> goalsPerMonthPerGame = gols.goalsPerGamePerMonth();
            goalsPerMonthPerGame.print();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }



}
