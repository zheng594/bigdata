package com.zheng.service.sql

import com.google.common.collect.Maps
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.plans.logical.{Project, SubqueryAlias}
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.execution.ProjectExec
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.hive.execution.{CreateHiveTableAsSelectCommand, InsertIntoHiveTable}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util

/**
 * Created by zheng on 2023-02-11.
 */
object SqlParseUtils {
    def main(args: Array[String]): Unit = {
        val sparkSession = SparkSession
            .builder.enableHiveSupport()
            .appName("SparkSql")
            .master("local[1]")
            .getOrCreate()
        var sql = "select * from default.sales_child"
        sql = "select * from (select tmp2+sales_region,tmp from (select concat(s.the_year,c.day_of_month) tmp ,(s.total_children+c.unit_sales) tmp2 ,s.sales_district ,c.sales_region from default.sales_child s left join sales_child c on s.sales_region=c.sales_region) a) b"
        sql = "with data as (select * from (select tmp2+sales_region,tmp from (select concat(s.the_year,c.day_of_month) tmp ,(s.total_children+c.unit_sales) tmp2 ,s.sales_district ,c.sales_region from default.sales_child s left join sales_child c on s.sales_region=c.sales_region) a) b)" +
            "select * from data \n"
            sql = "create table if not exists  tmp_tab as select tmp,ss from (select concat(s.the_year,c.day_of_month) tmp,s.yearly_income ss ,(s.total_children+c.unit_sales) tmp2 from default.sales_child s left join bi_sales c on s.sales_region=c.sales_region) a"
        //        sql = "create table if not exists  tmp_tab as select concat(s.the_year,c.day_of_month) ,s.yearly_income ss ,(s.total_children+c.unit_sales) ,s.the_year from default.sales_child s left join bi_sales c on s.sales_region=c.sales_region"
                    sql = "insert into default.bi_sales  select *  from default.sales_child"
        //            sql = "insert into default.tmp_tab  values(1,2)" //InsertIntoHiveTable

        //        var df = sparkSession.read.format("csv").option("header", "true").load("file:///Users/zheng/Downloads/test.csv")
        //    tmpDF.write.mode(SaveMode.Append).format("csv").insertInto("test_tab")
        //      df.saveAsTable("csv_tab")
        val df = sparkSession.sql("insert into default.bi_sales select *  from default.sales_child")
        val sibObj = SqlParseUtils.getSibship(df, "db.tab")
        println(sibObj)
    }

    /**
     * 支持语法：insert……as select、create …… as select…… 、select …… 、with ……
     * 获取字段血缘
     *
     * @param df
     * @param fullTableName 库名.表名
     * @return
     */
    def getSibship(df: DataFrame, fullTableName: String): SibObj = {
        val sibObj = new SibObj()
        var sqlType = "select"

        var outputDatabase = ""
        var outputTableName = ""
        val arr = fullTableName.split("\\.")
        if (arr.length == 1) {
            outputDatabase = "defult"
            outputTableName = arr(0)
        } else if (arr.length == 2) {
            outputDatabase = arr(0)
            outputTableName = arr(1)
        }

        val query = df.queryExecution
        val sparkPlan = query.sparkPlan

        //平铺获得所有字段对应的表
        val tableMap = Maps.newHashMap[String, String]();
        getColTabMap(query.analyzed.children, tableMap)

        val sibList = new java.util.ArrayList[SibTab]
        var projectList: Seq[NamedExpression] = null
        if (sparkPlan.isInstanceOf[ProjectExec]) { //select ……
            projectList = sparkPlan.asInstanceOf[ProjectExec].projectList
        } else if (sparkPlan.isInstanceOf[DataWritingCommandExec]) { //create as select  ……,insert  …… select
            val dw = sparkPlan.asInstanceOf[DataWritingCommandExec]

            if (dw.cmd.isInstanceOf[InsertIntoHiveTable]) {
                sqlType = "insert_select"
                val identifier = dw.cmd.asInstanceOf[InsertIntoHiveTable].table.identifier
                outputDatabase = identifier.database.get
                outputTableName = identifier.table

                if (!dw.cmd.query.isInstanceOf[Project]) {
                    sibObj.sqlType = "insert"
                    return sibObj
                }
                projectList = dw.cmd.query.asInstanceOf[Project].projectList
            } else if (dw.cmd.isInstanceOf[CreateHiveTableAsSelectCommand]) {
                sqlType = "create_select"

                val identifier = dw.cmd.asInstanceOf[CreateHiveTableAsSelectCommand].tableDesc.identifier
                outputDatabase = identifier.database.get
                outputTableName = identifier.table

                if (!dw.cmd.query.isInstanceOf[Project]) {
                    sibObj.sqlType = "create"
                    return sibObj
                }
                projectList = dw.cmd.query.asInstanceOf[Project].projectList
            }
        } else { //文件类型
            return sibObj;
        }

        projectList.foreach(project => {
            val sibTab = new SibTab()
            sibTab.db = outputDatabase
            sibTab.tableName = outputTableName
            sibTab.columnName = project.name

            val depends = new util.HashSet[SibTab]
            project.references.foreach(ref => {
                val arr = tableMap.get(ref.name + "#" + ref.exprId.id).split("\\.")

                val dep = new SibTab()
                dep.db = arr(0).toLowerCase()
                dep.tableName = arr(1).toLowerCase()
                dep.columnName = ref.name
                depends.add(dep)
            })
            sibTab.depends = depends

            sibList.add(sibTab)
        })

        sibObj.sqlType = sqlType
        sibObj.db = outputDatabase
        sibObj.tableName = outputTableName
        sibObj.columnSibshipList = sibList
        sibObj
    }

    /**
     * 获取所有表的所有字段
     *
     * @param childs
     * @param tableMap
     */
    def getColTabMap(childs: Seq[TreeNode[_]], tableMap: java.util.HashMap[String, String]): Unit = {
        childs.foreach(sub => {
            if (sub.isInstanceOf[SubqueryAlias] && sub.asInstanceOf[SubqueryAlias].child.isInstanceOf[HiveTableRelation]) {
                val relation = sub.asInstanceOf[SubqueryAlias].child.asInstanceOf[HiveTableRelation];
                val table = relation.tableMeta.identifier
                var tableName = table.table
                if (table.database != null && table.database != None) {
                    tableName = table.database.get + "." + tableName
                }
                relation.output.foreach(item => {
                    tableMap.put(item.name + "#" + item.exprId.id, tableName)
                })
            } else {
                getColTabMap(sub.children.asInstanceOf[Seq[TreeNode[_]]], tableMap)
            }
        })
    }
}

class SibObj {
    var sqlType = "" //insert_select,create_select,select,with
    var db = ""
    var tableName = ""
    var columnSibshipList: java.util.ArrayList[SibTab] = new util.ArrayList[SibTab]()
}

class SibTab {
    var db: String = ""
    var tableName: String = ""
    var columnName: String = ""
    var depends: util.HashSet[SibTab] = new util.HashSet[SibTab]

    def canEqual(other: Any): Boolean = other.isInstanceOf[SibTab]

    override def equals(other: Any): Boolean = other match {
        case that: SibTab =>
            (that canEqual this) &&
                db == that.db &&
                tableName == that.tableName &&
                columnName == that.columnName
        case _ => false
    }

    override def hashCode(): Int = {
        val state = Seq(db, tableName, columnName)
        state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
    }
}