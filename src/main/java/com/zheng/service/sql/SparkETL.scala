package com.zheng.service.sql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SparkETL {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val spark = sessionBuilder()
    val df = spark.sql(sqlStr())
    val plan = df.queryExecution.sparkPlan
    plan.id
    df.show(10)
    spark.close()
  }

  private def sessionBuilder(): SparkSession = {
    SparkSession.builder().appName("ETL").master("local")
      .config("spark.shuffle.consolidateFiles", "true")
      .config("spark.shuffle.file.buffer", "64k")
      .config("spark.reducer.maxSizeInFlight", "96m")
      .config("spark.shuffle.io.maxRetries", "10")
      .config("spark.shuffle.io.retryWait", "30s")
      .config("spark.sql.crossJoin.enabled", "true")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .enableHiveSupport()
      .getOrCreate()
  }

  private def sqlStr(): String = {
      """
        |select  a.id --主键ID
        |,a.operation             as quote_operation_type            --操作类型
        |,case when a.operation = 'CREATE' then '创建报价单'
        |when a.operation = 'GIVE_UP_QUOTE' then '取消报价' --未报价前返回取消报价
        |when a.operation = 'QUOTE' then '提交报价'
        |when a.operation = 'UNQUOTE' then '撤销报价'       --报完价之后撤销报价
        |when a.operation = 'RESULT_AUDIT_PASS' then '结果确认审核通过'
        |when a.operation = 'RESULT_AUDIT_UNPASS' then '结果确认审核不通过'
        |else '' end as quote_operation_type_name         --操作类型枚举
        |,a.quotation_id            as quotation_id             --报价单id
        |,b.quote_supplier_org_id   as quote_supplier_org_id    --报价供应商ID
        |,b.quote_supplier_org_name as quote_supplier_org_name  --报价供应商名称
        |,b.bidding_order_id        as bidding_order_id         --竞价单ID
        |,b.instance_code           as instance_code            --业务实例code
        |,b.instance_name           as instance_name            --业务实例名称
        |,b.district_code           as district_code            --区划code
        |,b.district_name           as district_name            --区划名称
        |,c.purchaser_org_ids       as purchaser_org_ids        --采购单位ID列表
        |,c.purchaser_org_names     as purchaser_org_names      --采购单位名称列表
        |,c.trade_model_code        as trade_model_code         --竞价模式code（只会有一个）
        |,c.trade_model_name        as trade_model_name         --竞价模式名称
        |,a.reason                  as quote_reason             --原因
        |,a.remark                  as quote_remark             --备注
        |,a.gmt_created             as gmt_created_time         --创建时间
        |from ods.ods_db_quote_quotation_history a --供应商报价历史表
        |join (select quotation_id
        |             ,quote_supplier_org_id
        |             ,quote_supplier_org_name
        |             ,bidding_order_id
        |             ,instance_code
        |             ,instance_name
        |             ,district_code
        |             ,district_name
        |      from dwd.dwd_trd_bid_quotation_detail_d where pt = '20230226') b on a.quotation_id = b.quotation_id --报价单事实表
        |join (select bidding_order_id
        |            ,purchaser_org_ids
        |            ,purchaser_org_names
        |            ,trade_model_code
        |            ,trade_model_name
        |     from dwd.dwd_trd_bid_bidding_order_detail_d
        |     where pt = '20230226') c on b.bidding_order_id = c.bidding_order_id --竞价单事实表
        |where a.is_del = 0
        |""".stripMargin
  }
}
