/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.simulator.tests.map;

import com.hazelcast.core.IMap;
import com.hazelcast.json.Json;
import com.hazelcast.json.JsonObject;
import com.hazelcast.json.JsonValue;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;
import java.util.Random;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;
import static org.apache.commons.lang3.RandomUtils.nextDouble;
import static org.apache.commons.lang3.RandomUtils.nextInt;
import static org.apache.commons.lang3.RandomUtils.nextLong;

public class JsonPutGetTest extends HazelcastTest {

    public String strategy = SerializationStrategyTest.Strategy.JSON.name();
    public int itemCount = 100000;
    public boolean useIndex = false;
    public String mapname = "default";

    private IMap<Integer, Object> map;

    private Object[] values;

    @Setup
    public void setUp() {
        map = targetInstance.getMap(mapname);
    }

    @Prepare
    public void prepare() {
        if (useIndex) {
            map.addIndex("stringVal", false);
        }

        Random random = new Random();
        values = new Object[itemCount];
        for (int i = 0; i < values.length; i++) {
            values[i] = createObject();
        }

        Streamer<Integer, Object> streamer = StreamerFactory.getInstance(map);
        for (int i = 0; i < itemCount; i++) {
            streamer.pushEntry(i, values[random.nextInt(values.length)]);
        }
        streamer.await();
    }

    @TimeStep(prob = 1.0)
    public void put(ThreadState state) {
        map.put(state.randomKey(), state.randomValue());
    }

    @TimeStep(prob = 0.0)
    public void set(ThreadState state) {
        map.set(state.randomWriteKey(), state.randomValue());
    }

    @TimeStep(prob = -1)
    public void get(ThreadState state) {
        map.get(state.randomKey());
    }

    public class ThreadState extends BaseThreadState {

        private int randomKey() {
            return randomInt(itemCount);
        }

        private int randomWriteKey() {
            return randomInt(itemCount);
        }

        private Object randomValue() {
            return values[randomInt(values.length)];
        }
    }

    @Teardown
    public void tearDown() {
        map.destroy();
    }

    private Object createObject() {
        return createJsonObject("key");
    }

    private JsonValue createJsonObject(String key) {
        JsonObject o = Json.object();
        o.set("_id", 0);
        o.set("key", key);
        o.set("stringVal", randomAlphanumeric(7));
        o.set("doubleVal", nextDouble(0.0, Double.MAX_VALUE));
        o.set("longVal", nextLong(0, 500));
        o.set("intVal", nextInt(0, Integer.MAX_VALUE));
        return o;
    }


}
