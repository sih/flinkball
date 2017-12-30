package eu.waldonia.labs.flinkball;

import java.util.Objects;

/**
 * A lowest common denominator object holding attributes found in all of the input CSV result files.
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

    public FootyResult(String division,
                       String date,
                       String homeTeam,
                       String awayTeam,
                       int fullTimeHomeGoals,
                       int fullTimeAwayGoals,
                       String fullTimeResult) {
        this.division = division;
        this.homeTeam = homeTeam;
        this.awayTeam = awayTeam;
        this.fullTimeHomeGoals = fullTimeHomeGoals;
        this.fullTimeAwayGoals = fullTimeAwayGoals;
        this.fullTimeResult = fullTimeResult;

        /*
            The date format is a little bit dirty in that it can be dd/MM/yy or dd/MM/yyyy
            Could do this formatting via JodaTime and use the withPivotYear to account for Prem League
            years in the 90s but given:
                i) JodaTime isn't in the Flink distro so would have to be added to the cluster;
                ii) Doing this with Java DateTime would be a pain;
                iii) The data is easily tokenizable
            it's better to just parse the date.
          */

        String[] dateParts = date.split("/");
        String dd = dateParts[0];
        String MM = dateParts[1];
        String year = dateParts[2];

        if (year.length() < 4) {
            if (Integer.valueOf(year) < 99 && Integer.valueOf(year) > 93) {
                year = "19" + year;
            } else {
                year = "20"+year;
            }
        }

        this.date = year+"-"+MM+"-"+dd;

    }

    public int year() {
        return Integer.valueOf(date.split("-")[0]);
    }

    public int month() {
        return Integer.valueOf(date.split("-")[1]);
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
