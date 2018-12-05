package org.cora.registerhub.constant;

/**
 * @author Colin
 * @date 2018/11/26
 */
public class ZkConstants {

    public static final Integer SESSION_TIMEOUT = 30000;
    public static final Integer CONNECTION_TIMEOUT = 3000;
    public static final Integer TIMEOUT_RETRY_TIME = 4;
    public static final Integer TIMEOUT_RETRY_INTERVAL = 2000;
    public static final String PATH_SEPARATOR = "/";

    private ZkConstants() {
    }
}
