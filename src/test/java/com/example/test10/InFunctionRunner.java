package com.example.test10;

import java.util.Collections;

import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.functions.LocalRunner;

class InFunctionRunner {

    public static void main(String[] args) throws Exception {
        FunctionConfig functionConfig = new FunctionConfig();
        functionConfig.setName(InFunction.class.getSimpleName());
        functionConfig.setInputs(Collections.singletonList(Config.IN_TOPIC));
        functionConfig.setSubName(Config.IN_SUB);
        functionConfig.setClassName(InFunction.class.getName());
        functionConfig.setRuntime(FunctionConfig.Runtime.JAVA);

        LocalRunner localRunner = LocalRunner.builder().functionConfig(functionConfig).build();
        localRunner.start(false);
    }
}
