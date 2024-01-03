package com.zheng.service.demo

import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd, SparkListenerApplicationStart, SparkListenerBlockManagerAdded, SparkListenerBlockManagerRemoved, SparkListenerBlockUpdated, SparkListenerEnvironmentUpdate, SparkListenerEvent, SparkListenerExecutorAdded, SparkListenerExecutorBlacklisted, SparkListenerExecutorBlacklistedForStage, SparkListenerExecutorMetricsUpdate, SparkListenerExecutorRemoved, SparkListenerExecutorUnblacklisted, SparkListenerJobEnd, SparkListenerJobStart, SparkListenerNodeBlacklisted, SparkListenerNodeBlacklistedForStage, SparkListenerNodeUnblacklisted, SparkListenerSpeculativeTaskSubmitted, SparkListenerStageCompleted, SparkListenerStageExecutorMetrics, SparkListenerStageSubmitted, SparkListenerTaskEnd, SparkListenerTaskGettingResult, SparkListenerTaskStart, SparkListenerUnpersistRDD}
import org.apache.spark.sql.catalyst.catalog.{CreateTablePreEvent, DropTablePreEvent}


class ServerListener extends SparkListener {
    override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
        super.onStageCompleted(stageCompleted)
    }

    override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
        super.onStageSubmitted(stageSubmitted)
    }

    override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
        super.onTaskStart(taskStart)
    }

    override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult): Unit = {
        super.onTaskGettingResult(taskGettingResult)
    }

    override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
        if (taskEnd.taskMetrics != null && taskEnd.taskMetrics.inputMetrics != null) {
            val tableName = taskEnd.taskMetrics.inputMetrics
        }
        val finishTime = taskEnd.taskInfo.finishTime
        super.onTaskEnd(taskEnd)
    }

    override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
        super.onJobStart(jobStart)
    }

    override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
        val jobEndTime = jobEnd.time
        super.onJobEnd(jobEnd)
    }

    override def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate): Unit = {
        super.onEnvironmentUpdate(environmentUpdate)
    }

    override def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded): Unit = {
        super.onBlockManagerAdded(blockManagerAdded)
    }

    override def onBlockManagerRemoved(blockManagerRemoved: SparkListenerBlockManagerRemoved): Unit = {
        super.onBlockManagerRemoved(blockManagerRemoved)
    }

    override def onUnpersistRDD(unpersistRDD: SparkListenerUnpersistRDD): Unit = {
        super.onUnpersistRDD(unpersistRDD)
    }

    override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
        super.onApplicationStart(applicationStart)
    }

    override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
        super.onApplicationEnd(applicationEnd)
    }

    override def onExecutorMetricsUpdate(executorMetricsUpdate: SparkListenerExecutorMetricsUpdate): Unit = {
        super.onExecutorMetricsUpdate(executorMetricsUpdate)
    }

    override def onStageExecutorMetrics(executorMetrics: SparkListenerStageExecutorMetrics): Unit = {
        super.onStageExecutorMetrics(executorMetrics)
    }

    override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
        super.onExecutorAdded(executorAdded)
    }

    override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
        super.onExecutorRemoved(executorRemoved)
    }

    override def onExecutorBlacklisted(executorBlacklisted: SparkListenerExecutorBlacklisted): Unit = {
        super.onExecutorBlacklisted(executorBlacklisted)
    }

    override def onExecutorBlacklistedForStage(executorBlacklistedForStage: SparkListenerExecutorBlacklistedForStage): Unit = {
        super.onExecutorBlacklistedForStage(executorBlacklistedForStage)
    }

    override def onNodeBlacklistedForStage(nodeBlacklistedForStage: SparkListenerNodeBlacklistedForStage): Unit = {
        super.onNodeBlacklistedForStage(nodeBlacklistedForStage)
    }

    override def onExecutorUnblacklisted(executorUnblacklisted: SparkListenerExecutorUnblacklisted): Unit = {
        super.onExecutorUnblacklisted(executorUnblacklisted)
    }

    override def onNodeBlacklisted(nodeBlacklisted: SparkListenerNodeBlacklisted): Unit = {
        super.onNodeBlacklisted(nodeBlacklisted)
    }

    override def onNodeUnblacklisted(nodeUnblacklisted: SparkListenerNodeUnblacklisted): Unit = {
        super.onNodeUnblacklisted(nodeUnblacklisted)
    }

    override def onBlockUpdated(blockUpdated: SparkListenerBlockUpdated): Unit = {
        super.onBlockUpdated(blockUpdated)
    }

    override def onSpeculativeTaskSubmitted(speculativeTask: SparkListenerSpeculativeTaskSubmitted): Unit = {
        super.onSpeculativeTaskSubmitted(speculativeTask)
    }

    override def onOtherEvent(event: SparkListenerEvent): Unit = {
        event match {
            case event: CreateTablePreEvent=>{
                val database = event.database;
                val tableName = event.name;
            }
            case event: DropTablePreEvent=>{
                val database = event.database;
                val tableName = event.name;
            }
        }
        super.onOtherEvent(event)
    }
}


