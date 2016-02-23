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
package org.jclouds.kinetic.util;

import com.google.common.base.Strings;
import com.google.common.util.concurrent.Uninterruptibles;
import org.jclouds.kinetic.reference.KineticConstants;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.nio.file.*;
import java.nio.file.attribute.*;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static java.nio.file.FileSystems.getDefault;

/**
 * Utilities for the kinetic blobstore.
 */
public class Utils {
   /** Private constructor for utility class. */
   private Utils() {
      // Do nothing
   }

   /**
    * Determine if Java is running on a Mac OS
    */
   public static boolean isMacOSX() {
      String osName = System.getProperty("os.name");
      return osName.contains("OS X");
   }

    /**
     * Determine the number of chunks a file of the given size will need.
     * @param size The file size for which we need the number of chunks.
     * @return The number of chunks needed to store a file of that size, including its metadata.
     */
   public static int numberOfChunksForSize(long size) {
      long actualChunkDataSize = KineticConstants.PROPERTY_CHUNK_SIZE_BYTES - KineticConstants
              .PROPERTY_CHUNK_FULL_HEADER_SIZE_BYTES;
      return (int)Math.ceil(size / actualChunkDataSize);
   }

   /**
    * Determine if Java is running on a windows OS
    */
   public static boolean isWindows() {
      return System.getProperty("os.name", "").toLowerCase().contains("windows");
   }

   /** Delete a file or a directory recursively. */
   public static void deleteRecursively(File file) throws IOException {
      if (file.isDirectory()) {
         File[] children = file.listFiles();
         if (children != null) {
            for (File child : children) {
               deleteRecursively(child);
            }
         }
      }

      delete(file);
   }

   public static String padHeader(String header, Object value) {
      Field headerSizeProperty;
      int headerSize;
      try {
         headerSizeProperty = KineticConstants.class.getDeclaredField("PROPERTY_" + header + "_HEADER_SIZE_BYTES");
         headerSize = headerSizeProperty.getInt(null);
      } catch (NoSuchFieldException nsfe) {
         throw new IllegalArgumentException("Field does not exist");
      } catch (IllegalAccessException iae) {
           /* If not specified in config, assume it doesn't need to be padded. */
         headerSize = 0;
      }
      return padStringValue(String.valueOf(value), headerSize);
   }

   private static String padStringValue(String value, int length) {
      return Strings.padStart(value, length, '\0');
   }

   public static Map<String, String> getChunkHeaders(String container, String key, int chunkId) {
      return getChunkHeaders(container + "/" + key, chunkId);
   }

   public static Map<String, String> getChunkHeaders(String path, int chunkId) {
      Map<String, String> headers = new LinkedHashMap<String, String>();
      headers.put("Company-Hash",
              padHeader("COMPANY_HASH", KineticConstants.PROPERTY_COMPANY_HASH_HEADER));
      headers.put("Application-Hash",
              padHeader("APPLICATION_HASH", KineticConstants.PROPERTY_APPLICATION_HASH_HEADER));
      headers.put("File-Id", padHeader("FILE_ID", path));
      headers.put("Chunk-Id", padHeader("CHUNK_ID", chunkId));
      headers.put("Raid-Level",
              padHeader("RAID_LEVEL", KineticConstants.PROPERTY_RAID_LEVEL_HEADER));
      headers.put("Raid-Length",
              padHeader("RAID_LENGTH", KineticConstants.PROPERTY_RAID_LENGTH_HEADER));
      headers.put("Block-Type", padHeader("BLOCK_TYPE", "D"));
      headers.put("Content-Hash", padHeader("CONTENT_HASH", "Test"));

      return headers;
   }

   private String getChunkHeader(String headerName, String container, String key, int chunkId) {
      Map<String, String> headers = this.getChunkHeaders(container, key, chunkId);
      String headerValue = headers.get(headerName);
      if (null != headerValue) {
         return headerValue;
      }
      throw new IllegalArgumentException("Header does not exist");
   }

   public static void delete(File file) throws IOException {
      for (int n = 0; n < 10; n++) {
         try {
            Files.delete(file.toPath());
            if (Files.exists(file.toPath())) {
               Uninterruptibles.sleepUninterruptibly(200, TimeUnit.MILLISECONDS);
               continue;
            }
            return;
         } catch (DirectoryNotEmptyException dnee) {
            // A previous file delete operation did not finish before this call
            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
            continue;
         } catch (AccessDeniedException ade) {
            // The file was locked by antivirus, indexing, or another operation triggered by previous file modification
            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
            continue;
         } catch (NoSuchFileException nse) {
            return; // The file has been eventually deleted after a previous operation that failed. no-op
         }
      }
      // File could not be deleted multiple times. It is very likely locked in another process
      throw new IOException("Could not delete: " + file.toPath());
   }

   /**
    * @return Localized name for the "Everyone" Windows principal.
    */
   public static final String getWindowsEveryonePrincipalName() {
      if (isWindows()) {
         try {
            Process process = new ProcessBuilder("whoami", "/groups").start();
            try {
               String line;
               try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                  while ((line = reader.readLine()) != null) {
                     if (line.indexOf("S-1-1-0") != -1) {
                        return line.split(" ")[0];
                     }
                  }
               }
            } finally {
               process.destroy();
            }
         } catch (IOException e) {
         }
      }
      // Default/fallback value
      return "Everyone";
   }

   public static final String WINDOWS_EVERYONE = getWindowsEveryonePrincipalName();

   /**
    * @param path The path to a Windows file or directory.
    * @return true if path has permissions set to Everyone on windows. The exact permissions are not checked.
    */
   public static boolean isPrivate(Path path) throws IOException {
      UserPrincipal everyone = getDefault().getUserPrincipalLookupService()
            .lookupPrincipalByName(WINDOWS_EVERYONE);
      AclFileAttributeView aclFileAttributes = java.nio.file.Files.getFileAttributeView(
            path, AclFileAttributeView.class);
      for (AclEntry aclEntry : aclFileAttributes.getAcl()) {
         if (aclEntry.principal().equals(everyone)) {
            return false;
         }
      }
      return true;
   }

   /**
    * @param path Remove "Everyone" from this path's Windows ACL permissions.
    */
   public static void setPrivate(Path path) throws IOException {
      UserPrincipal everyone = getDefault().getUserPrincipalLookupService()
            .lookupPrincipalByName(WINDOWS_EVERYONE);
      AclFileAttributeView aclFileAttributes = java.nio.file.Files.getFileAttributeView(
            path, AclFileAttributeView.class);
      CopyOnWriteArrayList<AclEntry> aclList = new CopyOnWriteArrayList(aclFileAttributes.getAcl());
      for (AclEntry aclEntry : aclList) {
         if (aclEntry.principal().equals(everyone) && aclEntry.type().equals(AclEntryType.ALLOW)) {
            aclList.remove(aclEntry);
         }
      }
      aclFileAttributes.setAcl(aclList);
   }

   /**
    * @param path Add "Everyone" with read enabled to this path's Windows ACL permissions.
    */
   public static void setPublic(Path path) throws IOException {
      UserPrincipal everyone = getDefault().getUserPrincipalLookupService()
            .lookupPrincipalByName(WINDOWS_EVERYONE);
      AclFileAttributeView aclFileAttributes = java.nio.file.Files.getFileAttributeView(
            path, AclFileAttributeView.class);
      List<AclEntry> list = aclFileAttributes.getAcl();
      list.add(AclEntry.newBuilder().setPrincipal(everyone).setPermissions(
            AclEntryPermission.READ_DATA,
            AclEntryPermission.READ_ACL,
            AclEntryPermission.READ_ATTRIBUTES,
            AclEntryPermission.READ_NAMED_ATTRS)
            .setType(AclEntryType.ALLOW)
            .build());
      aclFileAttributes.setAcl(list);
   }
}
