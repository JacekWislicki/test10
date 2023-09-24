package com.example.test10;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class Config {

    public static final String SERVICE_URL = "pulsar://localhost:6650";

    public static final String IN_TOPIC = "persistent://public/test10/topic-in";
    public static final String OUT_TOPIC_1 = "persistent://public/test10/topic-out-1";
    public static final String OUT_TOPIC_2 = "persistent://public/test10/topic-out-2";

    public static final String IN_SUB = "in-sub";
    public static final String OUT_SUB = "out-sub";
}
