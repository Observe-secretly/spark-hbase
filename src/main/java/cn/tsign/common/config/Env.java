package cn.tsign.common.config;

public class Env {

    // private static String env = "release";
    // private static String env = "test";
    private static String env = "dev";

    public static String getEnvironment() {
        return env;
    }

}
