//package com.zheng.service.sql
//
//import com.google.common.collect.{Lists, Maps}
//import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
//import org.apache.spark.sql.catalyst.expressions.{Expression, NamedExpression}
//import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias
//import org.apache.spark.sql.catalyst.trees.TreeNode
//import org.apache.spark.sql.execution.ProjectExec
//import org.apache.spark.sql.execution.command.DataWritingCommandExec
//import org.apache.spark.sql.hive.execution.{CreateHiveTableAsSelectCommand, InsertIntoHiveTable}
//import org.apache.spark.sql.{DataFrame, SparkSession}
//
//import java.util
//import scala.collection.JavaConversions._
//
///**
// * Created by zheng on 2023-02-11.
// */
//object SqlUtils {
//    def main(args: Array[String]): Unit = {
//        var sparkSession = SparkSession
//            .builder.enableHiveSupport()
//            .appName("SparkSql")
//            .master("local[1]")
//            .getOrCreate()
//        var sql = "select * from default.sales_child"
//        sql = "select * from (select tmp2+sales_region,tmp from (select concat(s.the_year,c.day_of_month) tmp ,(s.total_children+c.unit_sales) tmp2 ,s.sales_district ,c.sales_region from default.sales_child s left join sales_child c on s.sales_region=c.sales_region) a) b"
//        //    sql = "create table if not exists  tmp_tab as select tmp,ss from (select concat(s.the_year,c.day_of_month) tmp,s.yearly_income ss (s.total_children+c.unit_sales) tmp2 from default.sales_child s left join bi_sales c on s.sales_region=c.sales_region) a"
//        //    sql = "create table if not exists  tmp_tab as select concat(s.the_year,c.day_of_month) tmp,s.yearly_income ss ,(s.total_children+c.unit_sales) ,s.the_year from default.sales_child s left join bi_sales c on s.sales_region=c.sales_region"
//        //    sql = "insert into default.bi_sales  select *  from default.sales_child"
//        //    sql = "insert into default.tmp_tab  values(1,2)" //InsertIntoHiveTable
//
//        //    var tmpDF = sparkSession.read.format("csv").option("header","true").load("/Users/zheng/Downloads/test.csv")
//        //    val df = tmpDF.write.mode(SaveMode.Append).format("csv")
//        //    tmpDF.write.mode(SaveMode.Append).format("csv").insertInto("test_tab")
//        //      df.saveAsTable("csv_tab")
//        val df = sparkSession.sql(sql)
//        val sibObj = SqlUtils.getSibship(df, "tab")
//        println()
//    }
//
//    /**
//     * 获取字段血缘
//     *
//     * @param df
//     * @return
//     */
//    def getSibship(df: DataFrame, table: String): SibObj = {
//        val sibObj = new SibObj()
//        var sqlType = "select"
//
//        var outputDatabase = ""
//        var outputTableName = ""
//        val arr = table.split("\\.")
//        if (arr.length == 1) {
//            outputDatabase = "defult"
//            outputTableName = arr(0)
//        } else if (arr.length == 2) {
//            outputDatabase = arr(0)
//            outputTableName = arr(1)
//        }
//
//        val query = df.queryExecution
//        val sparkPlan = query.sparkPlan
//
//        //平铺获得所有字段对应的表
//        val tableMap = Maps.newHashMap[String, String]();
//        getColTabMap(query.analyzed.children, tableMap)
//
//        val sibList = new java.util.ArrayList[SibTab]
//        val colMap = Maps.newHashMap[String, java.util.ArrayList[String]]();
//        val originMap = Maps.newHashMap[String, String]()
//        var outputColList = new util.ArrayList[String]()
//        if (sparkPlan.isInstanceOf[ProjectExec]) { //select ……
//            val list = Lists.newArrayList(sparkPlan);
//            getColRelation(list, colMap, originMap) //获取字段血缘关系
//
//            println()
//        } else if (sparkPlan.isInstanceOf[DataWritingCommandExec]) { //create as select  ……,insert  …… select
//            getColRelation(sparkPlan.children, colMap, originMap) //获取字段血缘关系
//            if (colMap.size() == 0) {
//                sibObj.sqlType = "insert"
//                return sibObj
//            }
//
//            val dw = sparkPlan.asInstanceOf[DataWritingCommandExec]
//
//            if (dw.cmd.isInstanceOf[InsertIntoHiveTable]) {
//                sqlType = "insert_select"
//                val identifier = dw.cmd.asInstanceOf[InsertIntoHiveTable].table.identifier
//                outputDatabase = identifier.database.get
//                outputTableName = identifier.table
//            } else if (dw.cmd.isInstanceOf[CreateHiveTableAsSelectCommand]) {
//                sqlType = "create_select"
//
//                val identifier = dw.cmd.asInstanceOf[CreateHiveTableAsSelectCommand].tableDesc.identifier
//                outputDatabase = identifier.database.get
//                outputTableName = identifier.table
//
//                //获取字段血缘
//                for (colName <- dw.cmd.outputColumnNames) {
//                    outputColList.add(colName)
//                }
//            }
//        }
//
//
//        //获取最外层字段
//        val nameMap = new java.util.HashMap[String, String]
//        colMap.get("-1").foreach(item => {
//            nameMap.put(item.split("#")(0), item)
//        })
//
//        for (colName <- outputColList) {
//            val name = nameMap.get(colName)
//            val depends = new util.HashSet[SibTab]
//            getDepends(name, colMap, originMap, tableMap, depends)
//
//            val sibTab = new SibTab()
//            sibTab.databaseName = outputDatabase
//            sibTab.tableName = outputTableName
//            sibTab.col = colName
//            sibTab.depends = depends
//            sibList.add(sibTab)
//        }
//
//        sibObj.sqlType = sqlType
//        sibObj.databaseName = outputDatabase
//        sibObj.tableName = outputTableName
//        sibObj.sibTabList = sibList
//        sibObj
//    }
//
//    /**
//     *
//     * @param name
//     * @param colMap    所有字段树状映射
//     * @param originMap 最内层原始字段映射
//     * @param tableMap  根据字段获取表名
//     * @param childSet
//     */
//    def getDepends(name: String, colMap: java.util.HashMap[String, java.util.ArrayList[String]], originMap: java.util.HashMap[String, String], tableMap: java.util.HashMap[String, String], childSet: util.HashSet[SibTab]): Unit = {
//        val list = colMap.get(name)
//        if (list == null) {
//            val arr = tableMap.get(name).split("\\.")
//
//            val sibTab = new SibTab()
//            sibTab.databaseName = arr(0).toLowerCase()
//            sibTab.tableName = arr(1).toLowerCase()
//            sibTab.col = name.split("#")(0).toLowerCase()
//            childSet.add(sibTab)
//            return
//        }
//        list.foreach(col => {
//            if (originMap.get(col) != null) {
//                val tableName = tableMap.get(col)
//                val arr = tableName.split("\\.")
//
//                val sibTab = new SibTab()
//                sibTab.databaseName = arr(0).toLowerCase()
//                sibTab.tableName = arr(1).toLowerCase()
//                sibTab.col = col.split("#")(0).toLowerCase()
//                childSet.add(sibTab)
//            } else {
//                println()
//            }
//            getDepends(col, colMap, originMap, tableMap, childSet)
//        })
//    }
//
//
//    def getColRelation(childs: Seq[TreeNode[_]], colMap: java.util.HashMap[String, java.util.ArrayList[String]], originMap: java.util.HashMap[String, String]): Unit = {
//        childs.foreach(child => {
//            if (child.isInstanceOf[ProjectExec]) {
//                val source = child.asInstanceOf[ProjectExec]
//                getColRelationDetail(source.projectList, "-1", colMap, originMap)
//            } else {
//                getColRelation(child.children.asInstanceOf[Seq[TreeNode[_]]], colMap, originMap)
//            }
//        })
//    }
//
//    /**
//     *
//     * @param childs
//     * @param parent
//     * @param colMap    字段树状map映射
//     * @param originMap 最内层字段
//     */
//    def getColRelationDetail(childs: Seq[Expression], parent: String, colMap: java.util.HashMap[String, java.util.ArrayList[String]], originMap: java.util.HashMap[String, String]): Unit = {
//        childs.foreach(ss => {
//            if (ss.isInstanceOf[NamedExpression]) {
//                val sub = ss.asInstanceOf[NamedExpression]
//                var list = colMap.get(parent)
//                if (list == null) {
//                    list = Lists.newArrayList[String]()
//                }
//                val key = sub.name + "#" + sub.exprId.id
//                list.add(key)
//                colMap.put(parent, list)
//
//                if (ss.children.size == 0) {
//                    originMap.put(key, key)
//                }
//
//                getColRelationDetail(sub.children, key, colMap, originMap)
//            } else {
//                var list = colMap.get(parent)
//                if (list == null) {
//                    list = Lists.newArrayList[String]()
//                }
//                list.add(ss.toString())
//                colMap.put(parent, list)
//
//                getColRelationDetail(ss.children, ss.toString(), colMap, originMap)
//
//            }
//        })
//    }
//
//    /**
//     * 获取所有表的所有字段
//     *
//     * @param childs
//     * @param tableMap
//     */
//    def getColTabMap(childs: Seq[TreeNode[_]], tableMap: java.util.HashMap[String, String]): Unit = {
//        childs.foreach(sub => {
//            if (sub.isInstanceOf[SubqueryAlias] && sub.asInstanceOf[SubqueryAlias].child.isInstanceOf[HiveTableRelation]) {
//                val relation = sub.asInstanceOf[SubqueryAlias].child.asInstanceOf[HiveTableRelation];
//                val table = relation.tableMeta.identifier
//                var tableName = table.table
//                if (table.database != null && table.database != None) {
//                    tableName = table.database.get + "." + tableName
//                }
//                relation.output.foreach(item => {
//                    tableMap.put(item.name + "#" + item.exprId.id, tableName)
//                })
//            } else {
//                getColTabMap(sub.children.asInstanceOf[Seq[TreeNode[_]]], tableMap)
//            }
//        })
//    }
//
//
//}
//
//class SibObj {
//    var sqlType = "" //insert_select,create_select,select,with
//    var databaseName = ""
//    var tableName = ""
//    var sibTabList: java.util.ArrayList[SibTab] = new util.ArrayList[SibTab]()
//}
//
//class SibTab {
//    var databaseName: String = ""
//    var tableName: String = ""
//    var col: String = ""
//    var depends: util.HashSet[SibTab] = new util.HashSet[SibTab]
//
//
//    def canEqual(other: Any): Boolean = other.isInstanceOf[SibTab]
//
//    override def equals(other: Any): Boolean = other match {
//        case that: SibTab =>
//            (that canEqual this) &&
//                databaseName == that.databaseName &&
//                tableName == that.tableName &&
//                col == that.col
//        case _ => false
//    }
//
//    override def hashCode(): Int = {
//        val state = Seq(databaseName, tableName, col)
//        state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
//    }
//}