package org.cora.registerhub.listener;

import org.apache.log4j.Logger;
import org.cora.registerhub.constant.ServiceConstants;
import org.cora.registerhub.constant.ZkConstants;
import org.cora.registerhub.register.ServiceRegister;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;
import org.springframework.web.method.HandlerMethod;
import org.springframework.web.servlet.mvc.method.RequestMappingInfo;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping;

import java.util.ArrayList;
import java.util.Map;

/**
 * @author Colin
 * @date 2018/11/26
 */

@Component
public class ServiceListener implements ApplicationListener<ContextRefreshedEvent> {

    private static final Logger LOGGER = Logger.getLogger(ServiceListener.class);

    @Autowired
    public ServiceRegister serviceRegister;

    /**
     * Handle an application event.
     *
     * @param event the event to respond to
     */
    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        ApplicationContext applicationContext = event.getApplicationContext();
        if (null != applicationContext.getParent()) {
            return;
        }

        String appName = System.getProperty("app.name");
        String nodeName = System.getProperty("node.name");
        String serverIp = System.getProperty("server.ip");
        String serverPort = System.getProperty("server.port");
        serviceRegister.unregister(appName, nodeName);

        RequestMappingHandlerMapping mapping = applicationContext.getBean(RequestMappingHandlerMapping.class);
        Map<RequestMappingInfo, HandlerMethod> infoMap = mapping.getHandlerMethods();
        for (Map.Entry<RequestMappingInfo, HandlerMethod> info : infoMap.entrySet()) {
            String pattern = new ArrayList<>(info.getKey().getPatternsCondition().getPatterns()).get(0);
            if (pattern.startsWith(ZkConstants.PATH_SEPARATOR)) {
                pattern = pattern.substring(1);
            }
            String serviceName = info.getValue().getMethod().getDeclaringClass().getSimpleName() +
                    ServiceConstants.SERVICE_SEPARATOR + info.getValue().getMethod().getName();
            String serverAddress =
                    serverIp + ServiceConstants.SERVICE_SEPARATOR + serverPort + ZkConstants.PATH_SEPARATOR + appName +
                            ZkConstants.PATH_SEPARATOR + pattern;
            serviceRegister.register(appName, nodeName, serviceName, serverAddress);
        }

        serviceRegister.register(appName, nodeName);
    }
}
