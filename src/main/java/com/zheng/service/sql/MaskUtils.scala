package com.zheng.service.sql

import com.google.common.collect.{Lists, Maps}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.expressions.{Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{Project, SubqueryAlias}
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.execution.ProjectExec
import org.apache.spark.sql.execution.datasources.LogicalRelation

import scala.collection.JavaConversions._

/**
 * Created by zheng on 2019-12-11.
 */
object MaskUtils {
  def main(args: Array[String]): Unit = {
    var sparkSession = SparkSession
      .builder.enableHiveSupport()
      .appName("SparkSql")
      //        .master("spark://zheng.local:7077") //提交到spark集群
      //            .master("yarn") //提交到yarn
      .master("local[1]")
      .getOrCreate()
    var sql = "select * from default.sales_child"
    sql = "select * from (select concat(s.the_year,c.day_of_month) tmp ,(s.total_children+c.unit_sales) tmp2 from default.sales_child s left join sales_child c on s.sales_region=c.sales_region) a"
    sql = "create table if not exists  tmp_tab as select tmp,ss from (select concat(s.the_year,c.day_of_month) tmp,s.yearly_income ss ,(s.total_children+c.unit_sales) tmp2 from default.sales_child s left join bi_sales c on s.sales_region=c.sales_region) a"
    sql = "create table if not exists  tmp_tab2 as select tmp,ss from (select concat(s.the_year,c.day_of_month) tmp,s.yearly_income ss ,(s.total_children+c.unit_sales) tmp2 from default.sales_child s left join bi_sales c on s.sales_region=c.sales_region) a"
    sql = "with tab as (select tmp,ss from (select concat(s.the_year,c.day_of_month) tmp,s.yearly_income ss ,(s.total_children+c.unit_sales) tmp2 from default.sales_child s left join bi_sales c on s.sales_region=c.sales_region) a)" +
        " insert overwrite table tmp_tab2 select * from tab  "

    //        var sql = "insert into tab select * from default.sales_child"
    MaskUtils.getMaskSql(sparkSession, sql)
  }

  def getMaskSql(sparkSession: SparkSession, sql: String): String = {
    val df = sparkSession.sql(sql)
    val query = df.queryExecution
    var allTabs = new java.util.HashSet[String]();
    val curDatabase = sparkSession.catalog.currentDatabase;
//            getAllTabs(query.logical.children, allTabs, curDatabase)

    val colMap = Maps.newHashMap[String, java.util.ArrayList[String]]();
    val sparkPlan = query.sparkPlan

    val newSql = new StringBuilder(sql)
    var colStr = ""

    //        val children = sparkPlan.children;
    //        if (children == null || children.size == 0) {
    //            val columns = Lists.newArrayList[String]();
    //            var exce = query.sparkPlan.asInstanceOf[FileSourceScanExec];
    //            exce.requiredSchema.foreach(field => columns.add(field.name))
    //            exce.relation.partitionSchema.foreach(field => columns.add(field.name))
    //            val table = query.sparkPlan.asInstanceOf[FileSourceScanExec].tableIdentifier.get
    //
    //            var tableName = table.table;
    //            if (table.database.isDefined) {
    //                tableName = table.database.get + "." + tableName;
    //            }
    //
    //            for (col <- columns) {
    //                val colName = tableName + "." + col;
    //                colStr +=  ".mask(m.`" + col + "') `" + col + "`,"
    //            }
    //        } else {
    val anylized = query.analyzed

    val tableMap = Maps.newHashMap[String, String]();
    getFullTabs(anylized.children, tableMap) //平铺获得所有字段对应的表
    if (sparkPlan.isInstanceOf[ProjectExec]) {
      val list = Lists.newArrayList(sparkPlan);
      getColRelation(list, colMap) //获取字段血缘关系
    } else {
      getColRelation(sparkPlan.children, colMap) //获取字段血缘关系
      println()
    }

    anylized.asInstanceOf[Project].projectList.map(item => {
      val tabs: java.util.concurrent.CopyOnWriteArrayList[String] = getTabs(item.name + "#" + item.exprId.id, colMap, tableMap)
      if (tabs.size() > 0) {
        colStr += "mask(m.`" + item.name + "') `" + item.name + "#" + item.exprId.id + "`,"
      } else {
        colStr += "`" + item.name + "`,"
      }
    })
    //        }
    //        newSql.replace(sql.indexOf("select ") + 7, sql.indexOf(" from "), colStr.substring(0, colStr.length - 1))
    //        print(newSql.toString())

    newSql.toString()
  }

  //    def getAllTabs(childs: Seq[TreeNode[_]], allTabs: java.util.HashSet[String], curDtatabase: java.lang.String): Unit = {
  //        childs.foreach(node => {
  //            if (node.isInstanceOf[UnresolvedRelation]) {
  //                val tab = node.asInstanceOf[UnresolvedRelation].tableIdentifier
  //                var tableName = tab.table;
  //                if (tab.database == None) {
  //                    tableName = curDtatabase + "." + tableName
  //                } else {
  //                    tableName = tab.database.get + "." + tableName
  //                }
  //                allTabs.add(tableName)
  //            } else {
  //                getAllTabs(node.children.asInstanceOf[Seq[TreeNode[_]]], allTabs, curDtatabase)
  //            }
  //
  //        })
  //
  //    }

  def getTabs(colName: String, colMap: java.util.HashMap[String, java.util.ArrayList[String]], tabMap: java.util.HashMap[String, String]): java.util.concurrent.CopyOnWriteArrayList[String] = {
    val colList = new java.util.concurrent.ConcurrentHashMap[String, String]
    getList(colName, colMap, colList)
    val tabList = new java.util.concurrent.CopyOnWriteArrayList[String]()

    for (col <- colList) {
      val tab = tabMap.get(col._1);
      if (tab != null) {
        tabList.add(tab + "." + col._1.split("#")(0))
      }
    }

    tabList
  }

  def getList(colName: String, colMap: java.util.HashMap[String, java.util.ArrayList[String]], allMap: java.util.concurrent.ConcurrentHashMap[String, String]): Unit = {
    allMap.put(colName, colName)
    val colList = colMap.get(colName)
    if (colList != null) {
      for (col: String <- colList) {
        allMap.put(col, col)
        getList(col, colMap, allMap)
      }
    }
  }

  def getColRelation(childs: Seq[TreeNode[_]], colMap: java.util.HashMap[String, java.util.ArrayList[String]]): Unit = {
    childs.foreach(child => {
      if (child.isInstanceOf[ProjectExec]) {
        val source = child.asInstanceOf[ProjectExec]
        getColRelationDetail(source.projectList, "-1", colMap)
      } else {
        getColRelation(child.children.asInstanceOf[Seq[TreeNode[_]]], colMap)
      }
    })
  }

  def getColRelationDetail(childs: Seq[Expression], parent: String, colMap: java.util.HashMap[String, java.util.ArrayList[String]]): Unit = {
    childs.foreach(ss => {
      if (ss.isInstanceOf[NamedExpression]) {
        val sub = ss.asInstanceOf[NamedExpression]
        var list = colMap.get(parent)
        if (list == null) {
          list = Lists.newArrayList[String]()
        }
        val key = sub.name + "#" + sub.exprId.id
        list.add(key)
        colMap.put(parent, list)

        getColRelationDetail(sub.children, key, colMap)
      } else {
        var list = colMap.get(parent)
        if (list == null) {
          list = Lists.newArrayList[String]()
        }
        list.add(ss.toString())
        colMap.put(parent, list)

        getColRelationDetail(ss.children, ss.toString(), colMap)

      }
    })
  }

  /**
   * 获取所有表的所有字段
   *
   * @param childs
   * @param tableMap
   */
  def getFullTabs(childs: Seq[TreeNode[_]], tableMap: java.util.HashMap[String, String]): Unit = {
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
        getFullTabs(sub.children.asInstanceOf[Seq[TreeNode[_]]], tableMap)
      }
    })
  }
}
