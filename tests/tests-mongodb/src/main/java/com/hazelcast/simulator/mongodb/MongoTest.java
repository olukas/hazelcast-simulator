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
import com.mongodb.client.model.Indexes;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang3.RandomStringUtils;
import org.bson.Document;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;
import static org.apache.commons.lang3.RandomUtils.nextDouble;
import static org.apache.commons.lang3.RandomUtils.nextInt;
import static org.apache.commons.lang3.RandomUtils.nextLong;

public class MongoTest extends MongoDBTest {

    public int itemCount = 100000;
    public boolean useIndex = false;
    public String databaseName = "test";
    public String collectionName = "readWriteTest";

    private MongoCollection<Document> col;
    private Set<String> uniqueStrings = new HashSet<String>();

    @Setup
    public void setUp() {
        if (itemCount <= 0) {
            throw new IllegalStateException("itemCount must be larger than 0");
        }

        MongoDatabase database = client.getDatabase(databaseName);
        col = database.getCollection(collectionName);
    }

    @Prepare(global = true)
    public void prepare() {
        if (useIndex) {
            col.createIndex(Indexes.ascending("stringVal"));
        }
        String[] strings = generateUniqueStrings(itemCount);

        for (int i = 0; i < itemCount; i++) {
            col.insertOne(createJsonObject(strings[i], i));
        }
    }

    private String[] generateUniqueStrings(int uniqueStringsCount) {
        Set<String> stringsSet = new HashSet<String>(uniqueStringsCount);
        do {
            String randomString = RandomStringUtils.randomAlphabetic(30);
            stringsSet.add(randomString);
        } while (stringsSet.size() != uniqueStringsCount);
        uniqueStrings.addAll(stringsSet);
        return stringsSet.toArray(new String[uniqueStringsCount]);
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

    @TimeStep(prob = 1)
    public void getByStringIndex(BaseThreadState state) throws InterruptedException {
        col.find(new BasicDBObject("stringVal", "sancar")).first();
    }

    @Teardown
    public void tearDown() {
        col.drop();
        client.close();
    }
}
