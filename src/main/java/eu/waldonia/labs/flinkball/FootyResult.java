package eu.waldonia.labs.flinkball;

import java.util.Objects;

/**
 * @author sih
 */
public class FootyResult {

// Div,Date,HomeTeam,AwayTeam,FTHG,FTAG,FTR,
    private String division;
    private String date;
    private String homeTeam;
    private String awayTeam;
    private int fullTimeHomeGoals;
    private int fullTimeAwayGoals;
    private String fullTimeResult;

    public FootyResult() {

    }

    public FootyResult(String division,
                       String date,
                       String homeTeam,
                       String awayTeam,
                       int fullTimeHomeGoals,
                       int fullTimeAwayGoals,
                       String fullTimeResult) {
        this.division = division;
        this.date = date;
        this.homeTeam = homeTeam;
        this.awayTeam = awayTeam;
        this.fullTimeHomeGoals = fullTimeHomeGoals;
        this.fullTimeAwayGoals = fullTimeAwayGoals;
        this.fullTimeResult = fullTimeResult;
    }


    @Override
    public String toString() {
        return "{\"_class\":\"FootyResult\", " +
                "\"division\":" + (division == null ? "null" : "\"" + division + "\"") + ", " +
                "\"date\":" + (date == null ? "null" : "\"" + date + "\"") + ", " +
                "\"homeTeam\":" + (homeTeam == null ? "null" : "\"" + homeTeam + "\"") + ", " +
                "\"awayTeam\":" + (awayTeam == null ? "null" : "\"" + awayTeam + "\"") + ", " +
                "\"fullTimeHomeGoals\":\"" + fullTimeHomeGoals + "\"" + ", " +
                "\"fullTimeAwayGoals\":\"" + fullTimeAwayGoals + "\"" + ", " +
                "\"fullTimeResult\":" + (fullTimeResult == null ? "null" : "\"" + fullTimeResult + "\"") +
                "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FootyResult that = (FootyResult) o;
        return fullTimeHomeGoals == that.fullTimeHomeGoals &&
                fullTimeAwayGoals == that.fullTimeAwayGoals &&
                Objects.equals(division, that.division) &&
                Objects.equals(date, that.date) &&
                Objects.equals(homeTeam, that.homeTeam) &&
                Objects.equals(awayTeam, that.awayTeam) &&
                Objects.equals(fullTimeResult, that.fullTimeResult);
    }

    @Override
    public int hashCode() {

        return Objects.hash(division, date, homeTeam, awayTeam);
    }

    public String getDivision() {
        return division;
    }

    public void setDivision(String division) {
        this.division = division;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getHomeTeam() {
        return homeTeam;
    }

    public void setHomeTeam(String homeTeam) {
        this.homeTeam = homeTeam;
    }

    public String getAwayTeam() {
        return awayTeam;
    }

    public void setAwayTeam(String awayTeam) {
        this.awayTeam = awayTeam;
    }

    public int getFullTimeHomeGoals() {
        return fullTimeHomeGoals;
    }

    public void setFullTimeHomeGoals(int fullTimeHomeGoals) {
        this.fullTimeHomeGoals = fullTimeHomeGoals;
    }

    public int getFullTimeAwayGoals() {
        return fullTimeAwayGoals;
    }

    public void setFullTimeAwayGoals(int fullTimeAwayGoals) {
        this.fullTimeAwayGoals = fullTimeAwayGoals;
    }

    public String getFullTimeResult() {
        return fullTimeResult;
    }

    public void setFullTimeResult(String fullTimeResult) {
        this.fullTimeResult = fullTimeResult;
    }
}
