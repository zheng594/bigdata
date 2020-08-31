package com.zheng.service;

import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * yarn相关操作
 * Created by zheng on 2020/8/27
 */
@Slf4j
public class YarnManager {
    private YarnClient yarnClient;

    private void init() {
        Configuration conf = new YarnConfiguration();
        Configuration configuration = new YarnConfiguration(conf);
        yarnClient = YarnClient.createYarnClient();
        yarnClient.init(configuration);
        yarnClient.start();
    }

    public ApplicationId getApplicationId(String applicationName) {
        Set<String> applicationTypes = Sets.newHashSet("SPARK");
        EnumSet<YarnApplicationState> applicationStates = EnumSet.allOf(YarnApplicationState.class);

        List<ApplicationReport> applicationReports = null;
        try {
            init();
            applicationReports = yarnClient.getApplications(applicationTypes, applicationStates);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            try {
                yarnClient.close();
            } catch (IOException e) {
                log.error(e.getMessage(), e);
            }
        }

        if (CollectionUtils.isNotEmpty(applicationReports)) {
            for (ApplicationReport applicationReport : applicationReports) {
                ApplicationId applicationId = applicationReport.getApplicationId();
                if (StringUtils.equals(applicationId.toString(), applicationName)) {
                    return applicationId;
                }
            }
        }

        return null;
    }

    public YarnApplicationState getApplicationState(ApplicationId applicationId) {
        YarnApplicationState yarnApplicationState = null;
        try {
            init();
            ApplicationReport applicationReport = yarnClient.getApplicationReport(applicationId);
            yarnApplicationState = applicationReport.getYarnApplicationState();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            try {
                yarnClient.close();
            } catch (IOException e) {
                log.error(e.getMessage(), e);
            }
        }

        return yarnApplicationState;
    }

    /**
     * 获取application状态
     * @param applicationName
     * @return
     */
    public YarnApplicationState getApplicationState(String applicationName) {
        ApplicationId applicationId = this.getApplicationId(applicationName);
        if(applicationId == null){
            return null;
        }
        return this.getApplicationState(applicationId);
    }

    /**
     * 终止application
     * @param applicationId
     */
    public void kill(ApplicationId applicationId) {
        try {
            init();
            yarnClient.killApplication(applicationId);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            try {
                yarnClient.close();
            } catch (IOException e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    public static void main(String[] args) {
        YarnManager yarnManager = new YarnManager();
        YarnApplicationState app = yarnManager.getApplicationState("application_1598857150932_0007");
        System.out.println(app);
    }
}
