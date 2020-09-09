package com.zheng.service.spark

import java.util
import java.util.{Base64, LinkedList}

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import lombok.extern.slf4j.Slf4j
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
 *
 * Created by zheng on 2020/8/14
 */
@Slf4j
object SparkSqlJob {
  private var sparkSession: SparkSession = null
//  System.setProperty("HADOOP_USER_NAME", "zheng")

  def init() = {
    if (sparkSession == null) {
      val conf = new SQLConf() //会自动读取resource目录下的xml配置
      sparkSession = SparkSession
        .builder.enableHiveSupport()
        .appName("SparkSql")
//        .master("spark://zheng.local:7077") //提交到spark集群
          .master("yarn") //提交到yarn
          .getOrCreate()
      val sc: SparkContext = sparkSession.sparkContext
      sc.setLogLevel("WARN")
    }
  }

  def runJob(sql: String): util.HashMap[String, Object] = {
    this.init()
    var df: Dataset[Row] = sparkSession.sql(sql)
    getData(df)
  }

  def getData(dataSet: Dataset[Row]): util.HashMap[String, Object] = {
    var list: Array[Row] = null
    try {
      val objectMapper = new ObjectMapper()
      objectMapper.registerModule(DefaultScalaModule)
      objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

      val schemaList = new LinkedList[String]()
      val columnNames = new LinkedList[String]()
      val typeNames = new LinkedList[String]()
      val fieldList = new util.LinkedList[StructField]()
      val schema = dataSet.schema
      for (index <- 0 until schema.fields.length) {
        val field = schema.fields(index)
        fieldList.add(field)
        columnNames.add(field.name)
        typeNames.add(field.dataType.typeName)
        schemaList.add(field.name)
      }

      list = dataSet.collect()
      val data = new LinkedList[java.util.Map[String, String]]()
      for (row <- list) {
        val map = new java.util.HashMap[String, String]()
        for (index <- 0 to (row.length - 1)) {
          var value: String = null
          val obj = row.get(index)
          val field = fieldList.get(index)
          val fieldType = field.dataType.typeName
          if (obj != null) {
            if (fieldType.contains("map") || fieldType.contains("array")
              || fieldType.contains("struct")) {

              value = objectMapper.writeValueAsString(obj)
            } else if ("binary" == fieldType && StringUtils.startsWith(field.name, "img_data")) {
              val bytes = row.getAs[Array[Byte]](index)
              value = Base64.getEncoder.encodeToString(bytes)
            } else {
              value = obj.toString
            }
          }
          map.put(schemaList.get(index), value)
        }
        data.add(map)
      }

      val map = new util.HashMap[String, Object]()
      map.put("schemas", schemaList)
      map.put("columnNames", columnNames)
      map.put("data", data)
      map.put("typeNames", typeNames)
      map;
    } catch {
      case e: Exception =>
        throw e
    } finally {
      list = null
    }
  }
}
