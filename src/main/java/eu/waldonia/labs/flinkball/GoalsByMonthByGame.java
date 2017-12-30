package eu.waldonia.labs.flinkball;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Demonstrates joining two datasets
 * @author sih
 */
public class GoalsByMonthByGame {

    // Note that can't reuse this here as when we join we will get an error that the two inputs have different execution contexts
//    private GoalsByMonth goalsByMonth;
    private ResultFileNormalizer normalizer;

    public GoalsByMonthByGame() {
        normalizer = new ResultFileNormalizer();
    }

    public DataSet<Tuple2<Integer, Double>> goalsPerGamePerMonth() throws Exception {

        DataSet<FootyResult> results = normalizer.normalize();


        DataSet<Tuple2<Integer,Integer>> golsPerMonth =
                results.map(new MapFunction<FootyResult, Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> map(FootyResult footyResult) throws Exception {
                        return new Tuple2<>(footyResult.month(),
                                footyResult.getFullTimeAwayGoals()+footyResult.getFullTimeHomeGoals());
                    }
                })
                        .groupBy(0)
                        .sum(1)
                ;

        DataSet<Tuple2<Integer,Integer>> gamesPerMonth =
                results.map(new MapFunction<FootyResult, Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> map(FootyResult footyResult) throws Exception {
                        return new Tuple2<>(footyResult.month(), 1); // every result represents a game
                    }
                })
                        .groupBy(0)
                        .sum(1)
                ;

        DataSet<Tuple2<Integer, Double>> goalsPerGamePerMatch =
                golsPerMonth.join(gamesPerMonth).where(0).equalTo(0)    // join the two datasets by month
                .map(new MapFunction<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>, Tuple2<Integer, Double>>() {
                    @Override
                    public Tuple2<Integer, Double> map(Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> joinedDataset) throws Exception {
                        Integer month = joinedDataset.f0.f0;    // get the month from the first dataset
                        // divide the goals per month (f0.f1) by the matches per month (f1.f1)
                        Double goalsPerGamePerMonth = (joinedDataset.f0.f1.doubleValue()/joinedDataset.f1.f1.doubleValue());
                        Double roundedGoalsPerGamePerMonth = Math.round(goalsPerGamePerMonth * 100D)/100D;
                        return new Tuple2<>(month,roundedGoalsPerGamePerMonth);
                    }
                });

        return goalsPerGamePerMatch;

    }

    public static void main(String[] args) {
        try {
            GoalsByMonthByGame gols = new GoalsByMonthByGame();
            DataSet<Tuple2<Integer,Double>> goalsPerMonthPerGame = gols.goalsPerGamePerMonth();
            goalsPerMonthPerGame.print();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }



}
