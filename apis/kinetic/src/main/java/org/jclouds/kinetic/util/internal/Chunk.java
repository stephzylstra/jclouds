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

import org.jclouds.kinetic.reference.KineticConstants;
import org.jclouds.kinetic.strategy.internal.KineticStorageStrategyImpl;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.jclouds.kinetic.util.internal.EncryptionUtils.decrypt;

/**
 * Created by Steph Zylstra on 10/02/2016.
 */
public class Chunk {

    Object metadata;
    private long fileId;
    private long chunkId;
    private String fileKey;
    private String hash;
    private long length;

    private byte[] data;

    private final KineticStorageStrategyImpl strategy;

    public Chunk(KineticStorageStrategyImpl strategy, long fileId, long chunkId) {
        this.strategy = checkNotNull(strategy, "Kinetic storage strategy - chunk");
        this.setFileId(fileId);
        this.setChunkId(chunkId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Chunk chunk = (Chunk) o;

        if (fileId != chunk.fileId) return false;
        if (chunkId != chunk.chunkId) return false;
        if (length != chunk.length) return false;
        if (!fileKey.equals(chunk.fileKey)) return false;
        return hash.equals(chunk.hash);

    }

    @Override
    public int hashCode() {
        int result = (int) (fileId ^ (fileId >>> 32));
        result = 31 * result + (int) (chunkId ^ (chunkId >>> 32));
        return result;
    }


    private byte[] getDecryptedData() {
        String encryptionType = strategy.getKineticEncryptionAlgorithm();
        if (encryptionType.equals("none")) {
            return this.data;
        }
        return decrypt(this.data, encryptionType);
    }

    public byte[] getData() {
        // unless otherwise specified assume we want decrypted data
        return this.getData(true);
    }

    public byte[] getData(boolean decrypted) {
        if (decrypted) {
            return this.getDecryptedData();
        }
        // otherwise, return raw data
        return this.data;
    }

    public void setData(byte[] data) {
        String encryptionType = this.strategy.getKineticEncryptionAlgorithm();
        if (encryptionType.equals("none")) {
            this.data = data;
        } else {
            this.data = EncryptionUtils.encrypt(data, encryptionType);
        }
    }

    public String getId() {
        return String.valueOf(this.hashCode());
    }

    public long getFileId() {
        return fileId;
    }

    private void setFileId(long fileId) {
        this.fileId = fileId;
    }

    public long getChunkId() {
        return chunkId;
    }

    private void setChunkId(long chunkId) {
        this.chunkId = chunkId;
    }

    public String getFileKey() {
        return fileKey;
    }

    private void setFileKey(String fileKey) {
        this.fileKey = fileKey;
    }

    public String getHeaderHash() {
        return hash;
    }

    private void setHash(String hash) {
        this.hash = hash;
    }

    public long getLength() {
        return length;
    }

    private void setLength(long length) {
        if (length >= 0 && length <= KineticConstants.PROPERTY_CHUNK_SIZE_BYTES - KineticConstants
                .PROPERTY_CHUNK_FULL_HEADER_SIZE_BYTES) {
            this.length = length;
        }
    }

    public void setMetadata(Object metadata) {
        this.metadata = metadata;
    }

    public boolean processChunk() {
        if (this.metadata != null) {
            System.out.printf("Processing chunk %s@%s\n", this.hashCode(), this.metadata);
            return true;
        }
        return false;
    }
}
