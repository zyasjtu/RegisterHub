package org.cora.registerhub.register;

import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.cora.registerhub.constant.ServiceConstants;
import org.cora.registerhub.constant.ZkConstants;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.Arrays;
import java.util.Date;

/**
 * @author Colin
 * @date 2018/11/26
 */

@Component
public class ServiceRegister {

    private static final Logger LOGGER = Logger.getLogger(ServiceRegister.class);

    @Value("${org.cora.registerhub.zookeeper.address}")
    private String zkAddress;

    private CuratorFramework zkClient;

    @PostConstruct
    private void init() {
        zkClient = CuratorFrameworkFactory.builder().connectString(zkAddress).namespace(ServiceConstants.ROOT_NAMESPACE)
                .retryPolicy(new RetryNTimes(ZkConstants.TIMEOUT_RETRY_TIME, ZkConstants.TIMEOUT_RETRY_INTERVAL))
                .connectionTimeoutMs(ZkConstants.CONNECTION_TIMEOUT).sessionTimeoutMs(ZkConstants.SESSION_TIMEOUT)
                .build();
        zkClient.start();
    }

    public void register(String appName, String nodeName, String serviceName, String serviceAddress) {
        String servicePath = ZkConstants.PATH_SEPARATOR + StringUtils
                .join(Arrays.asList(ServiceConstants.REGISTERED_NAMESPACE, appName, nodeName, serviceName),
                        ZkConstants.PATH_SEPARATOR);
        try {
            zkClient.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT)
                    .forPath(servicePath, serviceAddress.getBytes());
        } catch (KeeperException.NodeExistsException ex) {
            try {
                zkClient.setData().forPath(servicePath, serviceAddress.getBytes());
            } catch (Exception e) {
                LOGGER.error("register service exception!", e);
            }
        } catch (Exception e) {
            LOGGER.error("register service exception!", e);
        }
    }

    public void unregister(String appName, String nodeName) {
        try {
            String nodePath = ZkConstants.PATH_SEPARATOR + StringUtils
                    .join(Arrays.asList(ServiceConstants.REGISTERED_NAMESPACE, appName, nodeName),
                            ZkConstants.PATH_SEPARATOR);
            zkClient.delete().guaranteed().deletingChildrenIfNeeded().forPath(nodePath);

            nodePath = ZkConstants.PATH_SEPARATOR + StringUtils
                    .join(Arrays.asList(ServiceConstants.DISCOVERED_NAMESPACE, appName, nodeName),
                            ZkConstants.PATH_SEPARATOR);
            zkClient.delete().guaranteed().deletingChildrenIfNeeded().forPath(nodePath);
        } catch (Exception e) {
            LOGGER.error("unregister node exception!", e);
        }
    }

    public void register(String appName, String nodeName) {
        String nodePath = ZkConstants.PATH_SEPARATOR + StringUtils
                .join(Arrays.asList(ServiceConstants.DISCOVERED_NAMESPACE, appName, nodeName),
                        ZkConstants.PATH_SEPARATOR);
        try {
            zkClient.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT)
                    .forPath(nodePath, DateFormatUtils.format(new Date(), JSON.DEFFAULT_DATE_FORMAT).getBytes());
        } catch (KeeperException.NodeExistsException ex) {
            try {
                zkClient.setData()
                        .forPath(nodePath, DateFormatUtils.format(new Date(), JSON.DEFFAULT_DATE_FORMAT).getBytes());
            } catch (Exception e) {
                LOGGER.error("register service exception!", e);
            }
        } catch (Exception e) {
            LOGGER.error("register service exception!", e);
        }
    }

    @PreDestroy
    public void destroy() {
        zkClient.close();
    }
}
