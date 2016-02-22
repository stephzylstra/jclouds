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

import java.util.Arrays;

/**
 * Created by Steph Zylstra on 19/02/2016.
 */
public class EncryptionUtils {

    private static final String[] ALLOWED_CIPHERS = {
            "AES128",
            "AES192",
            "AES256"
    };

    private static void checkCipher(String cipher) {
        if (!Arrays.asList(EncryptionUtils.ALLOWED_CIPHERS).contains(cipher)) {
            throw new RuntimeException("Invalid Cipher");
        }
    }

    private static byte[] decryptAES256(byte[] encrypted) {
        // TODO: Implement actual decryption
        byte[] bytes = new byte[encrypted.length];
        Arrays.fill(bytes, (byte)'P');
        return bytes;
    }

    private static byte[] encryptAES256(byte[] encrypted) {
        // TODO: Implement actual encryption
        byte[] bytes = new byte[encrypted.length];
        Arrays.fill(bytes, (byte)'E');
        return bytes;
    }

    public static byte[] decrypt(byte[] data, String cipher) {
        EncryptionUtils.checkCipher(cipher);
        byte[] output;
        switch(cipher) {
            default: output = EncryptionUtils.decryptAES256(data);
                break;
        }
        return output;
    }

    public static byte[] encrypt(byte[] plaintext, String cipher) {
        EncryptionUtils.checkCipher(cipher);
        byte[] output;
        switch(cipher) {
            default: output = EncryptionUtils.encryptAES256(plaintext);
                break;
        }
        return output;
    }

}
