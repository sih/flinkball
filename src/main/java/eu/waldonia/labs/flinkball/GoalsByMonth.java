package eu.waldonia.labs.flinkball;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * @author sih
 */
public class GoalsByMonth {


    private ResultFileNormalizer normalizer;

    public GoalsByMonth() {
        normalizer = new ResultFileNormalizer();
    }

    public void goals() throws Exception {

        DataSet<FootyResult> results = normalizer.normalize();


        DataSet<Tuple2<Integer,Integer>> goalsByMonth =
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
        goalsByMonth.print();

    }

    public static void main(String[] args) {
        try {
            GoalsByMonth gols = new GoalsByMonth();
            gols.goals();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
