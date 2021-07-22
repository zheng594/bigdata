package com.zheng.test

import com.zheng.service.yarn.SparkSqlClient

/**
 * Created by zheng on 2020/4/14.
 */
object Test {

    def main(args: Array[String]): Unit = {
        //        var sql = "CREATE TABLE `sales` (\n  `the_year` int ,\n  `month_of_year` int  ,\n  `day_of_month` int,\n  `the_date` timestamp,\n  `SALES_DISTRICT` string,\n  `SALES_REGION` string,\n  `SALES_COUNTRY` string,\n  `yearly_income` string,\n  `total_children` int,\n  `member_card` string,\n  `num_cars_owned` int ,\n  `gender` string,\n  `store_sales` double,\n  `store_cost` double,\n  `unit_sales` double\n) COMMENT 'sales'";
        //        var sql = "drop table sales"
        //        var sql = "show create table sales"
        var sql = "select * from default.sales_csv"
        sql = "select * from ("+sql+") a limit 50"

        var df = SparkSqlClient.runJob(sql);
        print()
    }
}
