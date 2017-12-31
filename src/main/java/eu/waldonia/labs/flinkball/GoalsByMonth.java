package eu.waldonia.labs.flinkball;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * Demonstrates a simple aggregation
 * @author sih
 */
public class GoalsByMonth {


    private ResultFileNormalizer normalizer;

    public GoalsByMonth() {
        normalizer = new ResultFileNormalizer();
    }

    public DataSet<Tuple3<Integer,String, Integer>> goals() throws Exception {

        DataSet<FootyResult> results = normalizer.normalize();


        DataSet<Tuple3<Integer,String,Integer>> goalsByMonth =
                results.map(new MapFunction<FootyResult, Tuple3<Integer, String, Integer>>() {
                    @Override
                    public Tuple3<Integer, String, Integer> map(FootyResult footyResult) throws Exception {
                        return new Tuple3<>(
                                footyResult.month(),
                                footyResult.getDivision(),
                                footyResult.getFullTimeAwayGoals()+footyResult.getFullTimeHomeGoals());
                    }
                })
                        .groupBy(0,1)
                        .sum(2)
                ;

        return goalsByMonth;

    }

    public static void main(String[] args) {
        try {
            GoalsByMonth gols = new GoalsByMonth();
            DataSet<Tuple3<Integer,String,Integer>> goalsPerMonth = gols.goals();
            goalsPerMonth.print();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
