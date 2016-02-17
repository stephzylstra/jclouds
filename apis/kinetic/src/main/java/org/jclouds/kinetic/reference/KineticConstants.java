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
package org.jclouds.kinetic.reference;

import java.util.HashMap;
import java.util.LinkedHashMap;

/**
 * Common constants used in kinetic provider
 */
public final class KineticConstants {

    /** Specify the base directory where provider starts its file operations - must exists */
    public static final String PROPERTY_BASEDIR = "jclouds.kinetic.basedir";

    /** Specify if the Content-Type of a file should be autodetected if it is not set */
    public static final String PROPERTY_AUTO_DETECT_CONTENT_TYPE = "jclouds.kinetic.auto-detect-content-type";

    /** Specify the size of a chunk, in bytes, including header */
    public static final Integer PROPERTY_CHUNK_SIZE_BYTES = 1048576; // 1MB

    /** Specify the size of a chunk's header, in bytes. The header size is based on
     *  the sum of the relevant header fields.
     */
    public static final Integer PROPERTY_CHUNK_FULL_HEADER_SIZE_BYTES = 161; //4096;

    /** Specify the company ID - this needs to be recognised by the KDCC on the other end. */
    /** TODO: Read from configuration file */
    public static final String PROPERTY_COMPANY_HASH_HEADER = "1";

    /** Specify the size of the company ID header. */
    public static final Integer PROPERTY_COMPANY_HASH_HEADER_SIZE_BYTES = 32;

    /** Specify the application ID - this needs to be recognised by the KDCC on the other end. */
    /** TODO: Read from configuration file */
    public static final String PROPERTY_APPLICATION_HASH_HEADER = "1";

    /** Specify the size of the application ID header. */
    public static final Integer PROPERTY_APPLICATION_HASH_HEADER_SIZE_BYTES = 32;

    /** Specify the size of the file ID header. This does not need a corresponding
     *  file ID, as this will vary depending on the actual file.
     */
    public static final Integer PROPERTY_FILE_ID_HEADER_SIZE_BYTES = 16;

    /** Specify the size of the block (chunk) ID header. This does not need a
     *  corresponding chunk ID constant as this will be generated for each chunk.
     */
    public static final Integer PROPERTY_CHUNK_ID_HEADER_SIZE_BYTES = 16;

    /** Specify the RAID type required. */
    /** TODO: Support RAID configurations other than RAID5. */
    /** TODO: Read this from configuration file. */
    public static final String PROPERTY_RAID_LEVEL_HEADER = "RAID5";

    /** Specify the size of the RAID level header. */
    public static final Integer PROPERTY_RAID_LEVEL_HEADER_SIZE_BYTES = 8;

    /** Specify the RAID length - i.e. the amount of disks in each RAID
     *  group. This number must be compatible with the RAID level selected.
     */
    /** TODO: Read this from configuration file. */
    public static final String PROPERTY_RAID_LENGTH_HEADER = "10";

    /** Specify the size of the RAID length header, in bytes. */
    public static final Integer PROPERTY_RAID_LENGTH_HEADER_SIZE_BYTES = 8;

    /** Specify how large the file size component of the header is. It will need to be
     *  left-padded with zeroes to make it up to the right size.
     */
    public static final Integer PROPERTY_FILE_SIZE_HEADER_SIZE_BYTES = 8;

    /** Specify the size of the identifier that specifies whether data is
     *  regular or parity data.
     */
    public static final Integer PROPERTY_BLOCK_TYPE_HEADER_SIZE_BYTES = 1;

    /** Specify the size of the content-hash header field */
    public static final Integer PROPERTY_CONTENT_HASH_HEADER_SIZE_BYTES = 32;

    /** Specify the size of the length of this chunk. */
    public static final Integer PROPERTY_CHUNK_LENGTH_HEADER_SIZE_BYTES = 8;

    private KineticConstants() {
        throw new AssertionError("intentionally unimplemented");
    }
}
