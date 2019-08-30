package cn.tsign.common.util;

import java.util.Calendar;

public class StatisTimeUtil {

    private StatisTime statisTime;

    public StatisTimeUtil(long timestamp){
        Calendar c = Calendar.getInstance();
        String year = String.valueOf(c.get(Calendar.YEAR));
        String month = String.valueOf((c.get(Calendar.MONTH)
                                       + 1) < 10 ? ("0" + (c.get(Calendar.MONTH) + 1)) : (c.get(Calendar.MONTH) + 1));
        String day = String.valueOf(c.get(Calendar.DAY_OF_MONTH) < 10 ? "0"
                                                                        + (c.get(Calendar.DAY_OF_MONTH)) : c.get(Calendar.DAY_OF_MONTH));
        String hour = String.valueOf(c.get(Calendar.HOUR_OF_DAY) < 10 ? ("0"
                                                                         + c.get(Calendar.HOUR_OF_DAY)) : c.get(Calendar.HOUR_OF_DAY));

        this.statisTime = new StatisTime(year, month, day, hour);
    }

    public StatisTime get() {
        return statisTime;
    }

    public class StatisTime {

        private String statisYear;

        private String statisMonth;

        private String statisDay;

        private String statisHour;

        public StatisTime(String year, String month, String day, String hour){
            statisYear = year;
            statisMonth = year + month;
            statisDay = year + month + day;
            statisHour = year + month + day + hour;

        }

        public String getStatisYear() {
            return statisYear;
        }

        public void setStatisYear(String statisYear) {
            this.statisYear = statisYear;
        }

        public String getStatisMonth() {
            return statisMonth;
        }

        public void setStatisMonth(String statisMonth) {
            this.statisMonth = statisMonth;
        }

        public String getStatisDay() {
            return statisDay;
        }

        public void setStatisDay(String statisDay) {
            this.statisDay = statisDay;
        }

        public String getStatisHour() {
            return statisHour;
        }

        public void setStatisHour(String statisHour) {
            this.statisHour = statisHour;
        }

    }

}
