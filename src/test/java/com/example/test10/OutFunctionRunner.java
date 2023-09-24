package com.example.test10;

import java.util.List;

import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.functions.LocalRunner;

class OutFunctionRunner {

    public static void main(String[] args) throws Exception {
        FunctionConfig functionConfig = new FunctionConfig();
        functionConfig.setName(OutFunction.class.getSimpleName());
        functionConfig.setInputs(List.of(Config.OUT_TOPIC_1, Config.OUT_TOPIC_2));
        functionConfig.setSubName(Config.OUT_SUB);
        functionConfig.setClassName(OutFunction.class.getName());
        functionConfig.setRuntime(FunctionConfig.Runtime.JAVA);

        LocalRunner localRunner = LocalRunner.builder().functionConfig(functionConfig).build();
        localRunner.start(false);
    }
}
