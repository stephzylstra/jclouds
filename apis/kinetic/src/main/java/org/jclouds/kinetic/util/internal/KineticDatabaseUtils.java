/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jclouds.kinetic.util.internal;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static org.jclouds.kinetic.reference.KineticConstants.PROPERTY_KINETIC_DATABASE_URI;

/**
 * Created by Steph Zylstra on 22/02/2016.
 * Singleton pattern.
 */
public final class KineticDatabaseUtils {

    private static KineticDatabaseUtils instance;
    private Connection databaseConnection;

    protected KineticDatabaseUtils() {
        // Deliberately empty to defeat instantiation
    }

    public synchronized static KineticDatabaseUtils getInstance() throws SQLException{
        if (instance == null) {
            instance = new KineticDatabaseUtils();
            instance.initialise();
        }
        return instance;
    }

    private void initialise() throws SQLException {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException cnfe) {
            throw new SQLException("Database setup failed");
        }
        this.databaseConnection = DriverManager.getConnection(PROPERTY_KINETIC_DATABASE_URI, "test",
                "test");

    }

    public void addFileToDatabase(String path, long fileSize) throws SQLException {
        String query = "INSERT INTO KineticFiles(FileName, FileSize, DateAdded, DateModified) VALUES (?, ?, NOW(), NOW())";
        PreparedStatement statement = this.databaseConnection.prepareStatement(query);
        statement.setString(1, path);
        statement.setLong(2, fileSize);
        statement.execute();
    }

    /*
    public void addChunkToDatabase(Chunk chunk) throws SQLException {
        String query = "INSERT INTO FileChunks(CompanyId, FilePath, ChunkKeyHash, ChunkDataHash, ChunkData) VALUES(?, ?, ?, ?, ?)";
        PreparedStatement statement = this.databaseConnection.prepareStatement(query);
        statement.setInt(1, KineticConstants.PROPERTY_COMPANY_HASH_HEADER);
        statement.setString(2, chunk.getFileKey());
        statement.setString(3, chunk.getHeaderHash());
        statement.setString(4, chunk.getDataHash());
        statement.setBytes(5, chunk.getData());
        statement.execute();
    }

    public Map<String, byte[]> getChunksForPath(String path) throws SQLException {
        String findChunksQuery = "SELECT ChunkKeyHash FROM FileChunks WHERE CompanyId = ? AND FilePath = ?";
        PreparedStatement statement = this.databaseConnection.prepareStatement(findChunksQuery);
        statement.setInt(1, KineticConstants.PROPERTY_COMPANY_HASH_HEADER);
        statement.setString(2, path);
        ResultSet chunkHashes = statement.executeQuery();

        TreeMap<String, byte[]> chunks = new TreeMap<String, byte[]>();

        while (chunkHashes.next()) {

        }

        }*/
}
