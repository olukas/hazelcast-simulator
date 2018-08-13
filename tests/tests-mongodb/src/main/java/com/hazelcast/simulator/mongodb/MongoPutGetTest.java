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
package com.hazelcast.simulator.mongodb;

import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Indexes;
import java.util.Random;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;
import static org.apache.commons.lang3.RandomUtils.nextDouble;
import static org.apache.commons.lang3.RandomUtils.nextInt;
import static org.apache.commons.lang3.RandomUtils.nextLong;
import org.bson.Document;

public class MongoPutGetTest extends MongoDBTest {

    public int itemCount = 100000;
    public boolean useIndex = false;
    public String databaseName = "test";
    public String collectionName = "putGetTest";
    public Random random = new Random();

    private MongoCollection<Document> col;

    public int idArraySize = 3;
    private Document[][] values;

    @Setup
    public void setUp() {
        if (itemCount <= 0) {
            throw new IllegalStateException("itemCount must be larger than 0");
        }

        MongoDatabase database = client.getDatabase(databaseName);
        col = database.getCollection(collectionName);
    }

    @Prepare
    public void prepare() {
        if (useIndex) {
            col.createIndex(Indexes.ascending("stringVal"));
        }

        values = new Document[idArraySize][itemCount];
        for (int i = 0; i < idArraySize; i++) {
            for (int j = 0; j < itemCount; j++) {
                values[i][j] = createObject(j);
            }
        }

        for (int i = 0; i < itemCount; i++) {
            col.insertOne(values[0][i]);
        }
    }

    @TimeStep(prob = 1.0)
    public void put(ThreadState state) {
        int id = state.randomKey();
        col.replaceOne(Filters.eq("_id", id), state.randomValue(id));
    }

    @TimeStep(prob = -1)
    public void get(ThreadState state) {
        col.find(new BasicDBObject("_id", state.randomKey())).first();
    }

    public class ThreadState extends BaseThreadState {

        private int randomKey() {
            return randomInt(itemCount);
        }

        private Document randomValue(int id) {
            return values[randomInt(idArraySize)][id];
        }
    }

    @Teardown
    public void tearDown() {
        col.drop();
        client.close();
    }

    private Document createObject(int id) {
        return createJsonObject("key", id);
    }

    private Document createJsonObject(String key, int id) {
        Document json = new Document("_id", id)
                .append("key", key)
                .append("stringVal", randomAlphanumeric(7))
                .append("doubleVal", nextDouble(0.0, Double.MAX_VALUE))
                .append("longVal", nextLong(0, 500))
                .append("intVal", nextInt(0, Integer.MAX_VALUE));
        return json;
    }

}
