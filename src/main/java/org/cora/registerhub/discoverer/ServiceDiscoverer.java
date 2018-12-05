package org.cora.registerhub.discoverer;

import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.retry.RetryNTimes;
import org.apache.log4j.Logger;
import org.cora.registerhub.constant.ServiceConstants;
import org.cora.registerhub.constant.ZkConstants;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Colin
 * @date 2018/11/26
 */

@Component
public class ServiceDiscoverer {

    private static final Logger LOGGER = Logger.getLogger(ServiceDiscoverer.class);

    private static final Map<String, Map<String, List<String>>> SERVICE_MAP = new ConcurrentHashMap<>();

    @Value("${org.cora.registerhub.zookeeper.address}")
    private String zkAddress;

    private CuratorFramework zkClient;

    private TreeCache treeCache;

    private TreeCacheListener treeCacheListener = new TreeCacheListener() {
        @Override
        public void childEvent(CuratorFramework curatorFramework, TreeCacheEvent treeCacheEvent) {
            LOGGER.info(treeCacheEvent.getType() + " ==> " + treeCacheEvent.getData().getPath());
            // 监听结点：/discover, 结点树：/discovered/appName/nodeName
            if (null == treeCacheEvent.getData()) {
                return;
            }
            List<String> list = Arrays.asList(
                    StringUtils.split(treeCacheEvent.getData().getPath(), ZkConstants.PATH_SEPARATOR));
            if (3 != list.size()) {
                return;
            }

            switch (treeCacheEvent.getType()) {
                case NODE_ADDED:
                case NODE_UPDATED:
                case NODE_REMOVED:
                    synchronized (SERVICE_MAP) {
                        SERVICE_MAP.remove(list.get(1));
                        updateServiceMap(list.get(1));
                    }
                    LOGGER.info("update service map ==> " + treeCacheEvent.getData().getPath());
                    break;
                default:
            }
        }
    };

    @PostConstruct
    private void init() throws Exception {
        zkClient = CuratorFrameworkFactory.builder().connectString(zkAddress).namespace(ServiceConstants.ROOT_NAMESPACE)
                .retryPolicy(new RetryNTimes(ZkConstants.TIMEOUT_RETRY_TIME, ZkConstants.TIMEOUT_RETRY_INTERVAL))
                .connectionTimeoutMs(ZkConstants.CONNECTION_TIMEOUT).sessionTimeoutMs(ZkConstants.SESSION_TIMEOUT)
                .build();
        zkClient.start();

        treeCache = TreeCache.newBuilder(zkClient, ZkConstants.PATH_SEPARATOR + ServiceConstants.DISCOVERED_NAMESPACE)
                .setCacheData(Boolean.TRUE).build();
        treeCache.getListenable().addListener(treeCacheListener);
        treeCache.start();
    }

    private void updateServiceMap(String appName) {
        try {
            String appPath = ZkConstants.PATH_SEPARATOR + StringUtils
                    .join(Arrays.asList(ServiceConstants.REGISTERED_NAMESPACE, appName), ZkConstants.PATH_SEPARATOR);
            List<String> nodeNames = zkClient.getChildren().forPath(appPath);
            if (CollectionUtils.isEmpty(nodeNames)) {
                return;
            }

            for (String nodeName : nodeNames) {
                String nodePath = appPath + ZkConstants.PATH_SEPARATOR + nodeName;
                List<String> serviceNames = zkClient.getChildren().forPath(nodePath);
                if (CollectionUtils.isEmpty(serviceNames)) {
                    continue;
                }

                for (String serviceName : serviceNames) {
                    String servicePath = nodePath + ZkConstants.PATH_SEPARATOR + serviceName;
                    String serviceAddress = new String(zkClient.getData().forPath(servicePath));
                    if (SERVICE_MAP.containsKey(appName)) {
                        if (SERVICE_MAP.get(appName).containsKey(serviceName)) {
                            SERVICE_MAP.get(appName).get(serviceName).add(serviceAddress);
                        } else {
                            List<String> serviceAddressList = new ArrayList<>();
                            serviceAddressList.add(serviceAddress);
                            SERVICE_MAP.get(appName).put(serviceName, serviceAddressList);
                        }
                    } else {
                        List<String> serviceAddressList = new ArrayList<>();
                        serviceAddressList.add(serviceAddress);
                        Map<String, List<String>> map = new ConcurrentHashMap<>();
                        map.put(serviceName, serviceAddressList);
                        SERVICE_MAP.put(appName, map);
                    }
                }
            }

        } catch (Exception e) {
            LOGGER.error("update server map exception!", e);
        }
    }

    public List<String> getServiceAddressList(String appName, String serviceName) {
        return SERVICE_MAP.get(appName).get(serviceName);
    }

    public String getServiceAddress(String appName, String serviceName) {
        Integer index = new SecureRandom().nextInt(SERVICE_MAP.size());
        return SERVICE_MAP.get(appName).get(serviceName).get(index);
    }

    public Map<String, Map<String, List<String>>> getServiceMap() {
        return SERVICE_MAP;
    }

    @PreDestroy
    public void destroy() {
        zkClient.close();
        treeCache.close();
    }
}
