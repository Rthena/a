package com.ibeifeng.sparkproject.test;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class RandomDate {
    /**
     * 在beginDate和endDate之间获取一个随机日期作为开始日期
     * @param beginDate  日期开始范围
     * @param endDate   日期结束范围
     * @param randNum  开始日期和结束日期的之间的天数
     * @return 依据 开始日期和randnum 算出结算日期并返回
     */

    public static List<Date> randomDate(String beginDate, String endDate, int randNum) {
        List<Date> list = new ArrayList<Date>();
        Date startTime = null;
        Date endTime = null;
        try {
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
            Date start = format.parse(beginDate);// 构造开始日期
            Date end = format.parse(endDate);// 构造结束日期
            // getTime()表示返回自 1970 年 1 月 1 日 00:00:00 GMT 以来此 Date 对象表示的毫秒数。
            if (start.getTime() >= end.getTime()) {
                return null;
            }
            long date = random(start.getTime(), end.getTime());
            startTime =  new Date(date);
            //System.out.println(date);
            date = date + randNum * 24*60*60*1000;
            //System.out.println(date);
            endTime = new Date(date);

            //System.out.println(dateToString(startTime, "yyyy-MM-dd"));
            //System.out.println(dateToString(endTime, "yyyy-MM-dd"));

        } catch (Exception e) {
            e.printStackTrace();
        }
        list.add(startTime);
        list.add(endTime);
        return list;
    }

    private static long random(long begin, long end) {
        long rtn = begin + (long) (Math.random() * (end - begin));
        // 如果返回的是开始时间和结束时间，则递归调用本函数查找随机值
        if (rtn == begin || rtn == end) {
            return random(begin, end);
        }
        return rtn;
    }

    public static void main(String[] args) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        List<Date> dates = RandomDate.randomDate("2018-05-19", "2019-05-19", 365);

        System.out.println(sdf.format(dates.get(0))+ " "+ sdf.format(dates.get(1)));
    }
}
