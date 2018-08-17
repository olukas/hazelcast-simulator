/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.query.PartitionPredicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.simulator.hz.DataSerializablePojo;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;

public class MapIndexesTest extends HazelcastTest {

    public int itemCount = 100000;
    public boolean prePopulate = true;
    public int partitions = 271;
    private IMap<Integer, DataSerializablePojo> map;
    private DataSerializablePojo[] values;

    @Setup
    public void setUp() {
        map = targetInstance.getMap(name);
        values = new DataSerializablePojo[itemCount];
        for (int i = 0; i < itemCount; i++) {
            values[i] = new DataSerializablePojo(i);
        }
    }

    @Prepare(global = true)
    public void globalPrepare() {
        if (prePopulate) {
            for (int i = 0; i < itemCount; ++i) {
                map.put(i, values[i]);
            }
        }
    }

    @TimeStep(prob = 0.0)
    public void put(ThreadState state) {
        map.put(state.randomKey(), state.randomValue());
    }

    @TimeStep(prob = 0.0)
    public void get(ThreadState state) {
        map.get(state.randomKey());
    }

    @TimeStep(prob = 1.0)
    public void query(ThreadState state) {
        map.values(Predicates.equal("id", state.randomKey()));
    }

    @SuppressWarnings("unchecked")
    @TimeStep(prob = 0.0)
    public void partitionQuery(ThreadState state) {
        map.values(new PartitionPredicate(state.randomPartition(), Predicates.equal("id", state.randomKey())));
    }

    public class ThreadState extends BaseThreadState {

        private int randomKey() {
            return randomInt(itemCount);
        }

        private DataSerializablePojo randomValue() {
            return values[randomInt(itemCount)];
        }

        private int randomPartition() {
            return randomInt(partitions);
        }
    }
}
