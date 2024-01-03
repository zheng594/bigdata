package com.zheng.service.demo

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, SubqueryAlias}

import java.util

/**
 * Created by zheng on 2020/8/31
 */
object TableRelationship {
    def main(args: Array[String]): Unit = {
        var sparkSession = SparkSession.builder()
            .appName("TableRelationship")
            .master("local[*]")
            .enableHiveSupport()
            .getOrCreate()

        val sql =
        //            "create table sales_child1 as " +
            "select concat(gender_a,'_',num) outside,* from " +
                "(select concat(gender,'_',time) gender_a ,* from " +
                "(select concat(the_year,month_of_year) time,store_sales+store_cost num,* from bi_sales))"
        val df = sparkSession.sql(sql)
//        val query = df2.queryExecution
//        val sparkPlan = query.sparkPlan

//        val cmd= sparkPlan.
//        val project = analyzed.asInstanceOf[Project]
//
//        val colMap = new util.HashMap[String, String]()
//        getColMap(project, colMap)
//
//        print(1)
    }

    def getColMap(logicalPlan: LogicalPlan, colMap: util.HashMap[String, String]): Any = {
        var project: Project = null
        if (logicalPlan.isInstanceOf[Project]) {
            project = logicalPlan.asInstanceOf[Project]
        } else if (logicalPlan.isInstanceOf[SubqueryAlias]) {
            val child: LogicalPlan = logicalPlan.asInstanceOf[SubqueryAlias].child
            if (child.isInstanceOf[Project]) {
                project = child.asInstanceOf[Project]
            } else if (child.isInstanceOf[HiveTableRelation]) {
                val tab = child.asInstanceOf[HiveTableRelation]
                return
            }
        }

        val list = project.projectList
        list.foreach(project => {
            //            val dataType = project.dataType
            colMap.put(project.exprId.id + "", project.name)
            project.references.foreach(r => {
                colMap.put(r.exprId.id + "", r.name)
            })
        })

        getColMap(project.child, colMap)


    }
}
