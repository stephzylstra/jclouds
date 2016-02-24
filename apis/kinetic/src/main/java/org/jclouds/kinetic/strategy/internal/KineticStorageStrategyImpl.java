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
package org.jclouds.kinetic.strategy.internal;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.*;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.google.common.hash.HashingInputStream;
import com.google.common.io.ByteSource;
import com.google.common.io.Files;
import com.google.common.primitives.Longs;
import org.apache.commons.lang3.ArrayUtils;
import org.jclouds.blobstore.LocalStorageStrategy;
import org.jclouds.blobstore.domain.*;
import org.jclouds.blobstore.domain.internal.MutableStorageMetadataImpl;
import org.jclouds.blobstore.options.CreateContainerOptions;
import org.jclouds.blobstore.options.ListContainerOptions;
import org.jclouds.blobstore.reference.BlobStoreConstants;
import org.jclouds.domain.Location;
import org.jclouds.io.ContentMetadata;
import org.jclouds.io.Payload;
import org.jclouds.kinetic.predicates.validators.KineticBlobKeyValidator;
import org.jclouds.kinetic.predicates.validators.KineticContainerNameValidator;
import org.jclouds.kinetic.reference.KineticConstants;
import org.jclouds.kinetic.util.Utils;
import org.jclouds.kinetic.util.internal.Chunk;
import org.jclouds.kinetic.util.internal.KineticDatabaseUtils;
import org.jclouds.logging.Logger;
import org.jclouds.rest.annotations.ParamValidators;

import javax.annotation.Resource;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.UserDefinedFileAttributeView;
import java.sql.SQLException;
import java.util.*;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.io.BaseEncoding.base16;
import static java.nio.file.Files.*;
import static org.jclouds.kinetic.util.Utils.delete;
import static org.jclouds.kinetic.util.Utils.*;

//import static org.jclouds.kinetic.util.Utils.*

/**
 * KineticStorageStrategyImpl implements a blob store that stores objects
 * on the file system. Content metadata and user attributes are stored in
 * extended attributes if the file system supports them. Directory blobs
 * (blobs that end with a /) cannot have content, but otherwise appear in
 * LIST like normal blobs.
 */
public class KineticStorageStrategyImpl implements LocalStorageStrategy {

   private static final String XATTR_CACHE_CONTROL = "user.cache-control";
   private static final String XATTR_CONTENT_DISPOSITION = "user.content-disposition";
   private static final String XATTR_CONTENT_ENCODING = "user.content-encoding";
   private static final String XATTR_CONTENT_LANGUAGE = "user.content-language";
   private static final String XATTR_CONTENT_MD5 = "user.content-md5";
   private static final String XATTR_CONTENT_TYPE = "user.content-type";
   private static final String XATTR_EXPIRES = "user.expires";
   private static final String XATTR_USER_METADATA_PREFIX = "user.user-metadata.";
   private static final byte[] DIRECTORY_MD5 =
           Hashing.md5().hashBytes(new byte[0]).asBytes();

   private static final String BACK_SLASH = "\\";

   @Resource
   protected Logger logger = Logger.NULL;

   protected final Provider<BlobBuilder> blobBuilders;
   protected final String baseDirectory;
   protected final boolean autoDetectContentType;
   protected final KineticContainerNameValidator kineticContainerNameValidator;
   protected final KineticBlobKeyValidator kineticBlobKeyValidator;


   protected final String kineticEncryptionAlgorithm;
   private final Supplier<Location> defaultLocation;

    protected KineticStorageStrategyImpl(Provider<BlobBuilder> blobBuilders,
                                         @Named(KineticConstants.PROPERTY_BASEDIR) String baseDir,
                                         @Named(KineticConstants.PROPERTY_AUTO_DETECT_CONTENT_TYPE) boolean autoDetectContentType,
                                         KineticContainerNameValidator kineticContainerNameValidator,
                                         KineticBlobKeyValidator kineticBlobKeyValidator,
                                         Supplier<Location> defaultLocation) {
        /* TODO: Trace down why this constructor is actually needed. */
        this(blobBuilders, baseDir, autoDetectContentType, null, kineticContainerNameValidator,
                kineticBlobKeyValidator, defaultLocation);
    }

   @Inject
   protected KineticStorageStrategyImpl(Provider<BlobBuilder> blobBuilders,
                                        @Named(KineticConstants.PROPERTY_BASEDIR) String baseDir,
                                        @Named(KineticConstants.PROPERTY_AUTO_DETECT_CONTENT_TYPE) boolean autoDetectContentType,
                                        @Named(KineticConstants.PROPERTY_ENCRYPTION_ALGORITHM) String
                                                   kineticEncryptionAlgorithm,
                                        KineticContainerNameValidator kineticContainerNameValidator,
                                        KineticBlobKeyValidator kineticBlobKeyValidator,
                                        Supplier<Location> defaultLocation) {
      this.blobBuilders = checkNotNull(blobBuilders, "kinetic storage strategy blobBuilders");
      this.baseDirectory = checkNotNull(baseDir, "kinetic storage strategy base directory");
      this.autoDetectContentType = autoDetectContentType;
      this.kineticContainerNameValidator = checkNotNull(kineticContainerNameValidator,
            "kinetic container name validator");
      this.kineticBlobKeyValidator = checkNotNull(kineticBlobKeyValidator, "kinetic blob key validator");
      this.kineticEncryptionAlgorithm = checkNotNull(kineticEncryptionAlgorithm, "kinetic encryption algorithm");
      this.defaultLocation = defaultLocation;
      logger.info("Finished initialising KineticStorageStrategyImpl");
   }

    public String getKineticEncryptionAlgorithm() {
        return kineticEncryptionAlgorithm;
    }

   @Override
   public boolean containerExists(String container) {
      kineticContainerNameValidator.validate(container);
      return directoryExists(container, null);
   }

   @Override
   public Collection<String> getAllContainerNames() {
      File[] files = new File(buildPathStartingFromBaseDir()).listFiles();
      if (files == null) {
         return ImmutableList.of();
      }
      ImmutableList.Builder<String> containers = ImmutableList.builder();
      for (File file : files) {
         if (file.isDirectory()) {
            containers.add(file.getName());
         }
      }
      return containers.build();
   }

   @Override
   public boolean createContainerInLocation(String container, Location location, CreateContainerOptions options) {
      // TODO: implement location
      logger.debug("Creating container %s", container);
      kineticContainerNameValidator.validate(container);
      boolean created = createDirectoryWithResult(container, null);
      if (created) {
         setContainerAccess(container, options.isPublicRead() ? ContainerAccess.PUBLIC_READ : ContainerAccess.PRIVATE);
      }
      return created;
   }

   @Override
   public ContainerAccess getContainerAccess(String container) {
      Path path = new File(buildPathStartingFromBaseDir(container)).toPath();

      if ( isWindows() ) {
         try {
            if (isPrivate(path)) {
               return ContainerAccess.PRIVATE;
            } else {
               return ContainerAccess.PUBLIC_READ;
            }
         } catch (IOException e) {
            throw new RuntimeException(e);
         }
      } else {
         Set<PosixFilePermission> permissions;
         try {
            permissions = getPosixFilePermissions(path);
         } catch (IOException ioe) {
            throw Throwables.propagate(ioe);
         }
         return permissions.contains(PosixFilePermission.OTHERS_READ)
               ? ContainerAccess.PUBLIC_READ : ContainerAccess.PRIVATE;
      }
   }

   @Override
   public void setContainerAccess(String container, ContainerAccess access) {
      Path path = new File(buildPathStartingFromBaseDir(container)).toPath();

      if ( isWindows() ) {
         try {
            if (access == ContainerAccess.PRIVATE) {
               setPrivate(path);
            } else {
               setPublic(path);
            }
         } catch (IOException e) {
            throw new RuntimeException(e);
         }
      } else {
         Set<PosixFilePermission> permissions;
         try {
            permissions = getPosixFilePermissions(path);
            if (access == ContainerAccess.PRIVATE) {
               permissions.remove(PosixFilePermission.OTHERS_READ);
            } else if (access == ContainerAccess.PUBLIC_READ) {
               permissions.add(PosixFilePermission.OTHERS_READ);
            }
            setPosixFilePermissions(path, permissions);
         } catch (IOException ioe) {
            throw Throwables.propagate(ioe);
         }
      }
   }

   @Override
   public void deleteContainer(String container) {
      kineticContainerNameValidator.validate(container);
      if (!containerExists(container)) {
         return;
      }
      deleteDirectory(container, null);
   }

   @Override
   public void clearContainer(final String container) {
      clearContainer(container, ListContainerOptions.Builder.recursive());
   }

   @Override
   public void clearContainer(String container, ListContainerOptions options) {
      kineticContainerNameValidator.validate(container);
      if (options.getDir() != null) {
         container += denormalize("/" + options.getDir());
      }
      try {
         File containerFile = openFolder(container);
         File[] children = containerFile.listFiles();
         if (null != children) {
            for (File child : children)
               if (options.isRecursive() || child.isFile()) {
                  Utils.deleteRecursively(child);
               }
         }
      } catch (IOException e) {
         logger.error(e, "An error occurred while clearing container %s", container);
         Throwables.propagate(e);
      }
   }

   @Override
   public StorageMetadata getContainerMetadata(String container) {
      MutableStorageMetadata metadata = new MutableStorageMetadataImpl();
      metadata.setName(container);
      metadata.setType(StorageType.CONTAINER);
      metadata.setLocation(getLocation(container));
      Path path = new File(buildPathStartingFromBaseDir(container)).toPath();
      BasicFileAttributes attr;
      try {
         attr = readAttributes(path, BasicFileAttributes.class);
      } catch (IOException e) {
         throw Throwables.propagate(e);
      }
      metadata.setCreationDate(new Date(attr.creationTime().toMillis()));
      return metadata;
   }

   @Override
   public boolean blobExists(String container, String key) {
      kineticContainerNameValidator.validate(container);
      kineticBlobKeyValidator.validate(key);
      try {
         return buildPathAndChecksIfBlobExists(container, key);
      } catch (IOException e) {
         logger.error(e, "An error occurred while checking key %s in container %s",
               container, key);
         throw Throwables.propagate(e);
      }
   }

   /**
    * Returns all the blobs key inside a container
    *
    * @param container
    * @return
    * @throws IOException
    */
   @Override
   public Iterable<String> getBlobKeysInsideContainer(String container) throws IOException {
      kineticContainerNameValidator.validate(container);
      // check if container exists
      // TODO maybe an error is more appropriate
      Set<String> blobNames = Sets.newHashSet();
      if (!containerExists(container)) {
         return blobNames;
      }

      File containerFile = openFolder(container);
      final int containerPathLength = containerFile.getAbsolutePath().length() + 1;
      populateBlobKeysInContainer(containerFile, blobNames, new Function<String, String>() {
         @Override
         public String apply(String string) {
            return string.substring(containerPathLength);
         }
      });
      return blobNames;
   }

    private Blob createBlobFromByteSource(final String container, final String key, final ByteSource byteSource) {
        BlobBuilder builder = blobBuilders.get();
        builder.name(key);
        File file = getFileForBlobKey(container, key);
        try {
            String cacheControl = null;
            String contentDisposition = null;
            String contentEncoding = null;
            String contentLanguage = null;
            String contentType = null;
            HashCode hashCode = null;
            Date expires = null;
            ImmutableMap.Builder<String, String> userMetadata = ImmutableMap.builder();

            UserDefinedFileAttributeView view = getUserDefinedFileAttributeView(file.toPath());
            if (view != null) {
                Set<String> attributes = ImmutableSet.copyOf(view.list());

                cacheControl = readStringAttributeIfPresent(view, attributes, XATTR_CACHE_CONTROL);
                contentDisposition = readStringAttributeIfPresent(view, attributes, XATTR_CONTENT_DISPOSITION);
                contentEncoding = readStringAttributeIfPresent(view, attributes, XATTR_CONTENT_ENCODING);
                contentLanguage = readStringAttributeIfPresent(view, attributes, XATTR_CONTENT_LANGUAGE);
                contentType = readStringAttributeIfPresent(view, attributes, XATTR_CONTENT_TYPE);
                if (contentType == null && autoDetectContentType) {
                    contentType = probeContentType(file.toPath());
                }
                if (attributes.contains(XATTR_CONTENT_MD5)) {
                    ByteBuffer buf = ByteBuffer.allocate(view.size(XATTR_CONTENT_MD5));
                    view.read(XATTR_CONTENT_MD5, buf);
                    hashCode = HashCode.fromBytes(buf.array());
                }
                if (attributes.contains(XATTR_EXPIRES)) {
                    ByteBuffer buf = ByteBuffer.allocate(view.size(XATTR_EXPIRES));
                    view.read(XATTR_EXPIRES, buf);
                    buf.flip();
                    expires = new Date(buf.asLongBuffer().get());
                }
                for (String attribute : attributes) {
                    if (!attribute.startsWith(XATTR_USER_METADATA_PREFIX)) {
                        continue;
                    }
                    String value = readStringAttributeIfPresent(view, attributes, attribute);
                    userMetadata.put(attribute.substring(XATTR_USER_METADATA_PREFIX.length()), value);
                }

                builder.payload(byteSource)
                        .cacheControl(cacheControl)
                        .contentDisposition(contentDisposition)
                        .contentEncoding(contentEncoding)
                        .contentLanguage(contentLanguage)
                        .contentLength(byteSource.size())
                        .contentMD5(hashCode)
                        .contentType(contentType)
                        .expires(expires)
                        .userMetadata(userMetadata.build());
            } else {
                builder.payload(byteSource)
                        .contentLength(byteSource.size())
                        .contentMD5(byteSource.hash(Hashing.md5()).asBytes());
            }
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
        Blob blob = builder.build();
        blob.getMetadata().setContainer(container);
        blob.getMetadata().setLastModified(new Date(file.lastModified()));
        blob.getMetadata().setSize(file.length());
        if (blob.getPayload().getContentMetadata().getContentMD5() != null)
            blob.getMetadata().setETag(base16().lowerCase().encode(blob.getPayload().getContentMetadata().getContentMD5()));
        return blob;
    }
    
    private Blob getChunkedBlob(final String container, final String key, final long chunkId) {
        BlobBuilder builder = blobBuilders.get();
        builder.name(key);
        File file = getFileForBlobKey(container, key);

        try {
            List<String> chunkKeys = KineticDatabaseUtils.getInstance().getFileChunkKeysFromDatabase(file.getPath());
            byte[] blobData = new byte[0];
            for (String chunkKey : chunkKeys) {
                byte[] data = KineticDatabaseUtils.getInstance().getChunkFromDatabase(chunkKey);
                blobData = ArrayUtils.addAll(blobData, data);
            }

            return this.createBlobFromByteSource(container, key, ByteSource.wrap(blobData));

        } catch (SQLException sqle) {

            ByteSource byteSource;

            if (getDirectoryBlobSuffix(key) != null) {
                logger.debug("%s - %s is a directory", container, key);
                byteSource = ByteSource.empty();
            } else {
                byteSource = Files.asByteSource(file)
                        .slice(chunkId, KineticConstants.PROPERTY_CHUNK_SIZE_BYTES - KineticConstants.PROPERTY_CHUNK_FULL_HEADER_SIZE_BYTES);
            }

            return this.createBlobFromByteSource(container, key, byteSource);
        }
    }

   @Override
   public Blob getBlob(final String container, final String key) {
      BlobBuilder builder = blobBuilders.get();
      builder.name(key);
      File file = getFileForBlobKey(container, key);
      TreeMap<Long, Blob> blobs = new TreeMap<Long, Blob>();
      long fileLength = file.length();
      long currentByte = 0;
      while (currentByte < fileLength) {
          byte[] chunkContents = new byte[0];
          try {
              chunkContents = Files.asByteSource(file).slice(currentByte, KineticConstants
                      .PROPERTY_CHUNK_SIZE_BYTES -
                      KineticConstants.PROPERTY_CHUNK_FULL_HEADER_SIZE_BYTES).read();
          } catch (IOException e) {
              e.printStackTrace();
          }
          Chunk chunk = new Chunk(this, 0, currentByte);
          chunk.setData(chunkContents);
          chunk.processChunk();
          System.out.printf("Chunk Encoded: %s\n", Arrays.toString(chunk.getData(false)));
          System.out.printf("Chunk Decoded: %s\n", Arrays.toString(chunk.getData(true)));
          Blob chunkBlob = this.getChunkedBlob(container, key, currentByte);
          blobs.put(currentByte, chunkBlob);
          currentByte += KineticConstants.PROPERTY_CHUNK_SIZE_BYTES - KineticConstants.PROPERTY_CHUNK_FULL_HEADER_SIZE_BYTES;
      }
       List<ByteSource> byteSources = new ArrayList<ByteSource>();
       for (Map.Entry<Long, Blob> entry : blobs.entrySet()) {
           byteSources.add((ByteSource)(entry.getValue().getPayload().getRawContent()));
       }
       ByteSource finalByteSource = ByteSource.concat(byteSources);
       return createBlobFromByteSource(container, key, finalByteSource);
   }

   private void writeCommonMetadataAttr(UserDefinedFileAttributeView view, Blob blob) throws IOException {
      ContentMetadata metadata = blob.getMetadata().getContentMetadata();
      writeStringAttributeIfPresent(view, XATTR_CACHE_CONTROL, metadata.getCacheControl());
      writeStringAttributeIfPresent(view, XATTR_CONTENT_DISPOSITION, metadata.getContentDisposition());
      writeStringAttributeIfPresent(view, XATTR_CONTENT_ENCODING, metadata.getContentEncoding());
      writeStringAttributeIfPresent(view, XATTR_CONTENT_LANGUAGE, metadata.getContentLanguage());
      writeStringAttributeIfPresent(view, XATTR_CONTENT_TYPE, metadata.getContentType());
      Date expires = metadata.getExpires();
      if (expires != null) {
         ByteBuffer buf = ByteBuffer.allocate(Longs.BYTES).putLong(expires.getTime());
         buf.flip();
         view.write(XATTR_EXPIRES, buf);
      }
      for (Map.Entry<String, String> entry : blob.getMetadata().getUserMetadata().entrySet()) {
         writeStringAttributeIfPresent(view, XATTR_USER_METADATA_PREFIX + entry.getKey(), entry.getValue());
      }
   }

   private String putDirectoryBlob(final String containerName, final Blob blob) throws IOException {
      String blobKey = blob.getMetadata().getName();
      ContentMetadata metadata = blob.getMetadata().getContentMetadata();
      Long contentLength = metadata.getContentLength();
      if (contentLength != null && contentLength != 0) {
         throw new IllegalArgumentException(
                 "Directory blob cannot have content: " + blobKey);
      }
      File outputFile = getFileForBlobKey(containerName, blobKey);
      Path outputPath = outputFile.toPath();
      if (!outputFile.isDirectory() && !outputFile.mkdirs()) {
         throw new IOException("Unable to mkdir: " + outputPath);
      }

      UserDefinedFileAttributeView view = getUserDefinedFileAttributeView(outputPath);
      if (view != null) {
         try {
            view.write(XATTR_CONTENT_MD5, ByteBuffer.wrap(DIRECTORY_MD5));
            writeCommonMetadataAttr(view, blob);
         } catch (IOException e) {
            logger.debug("xattrs not supported on %s", outputPath);
         }
      } else {
         logger.warn("xattr not supported on %s", blobKey);
      }

      return base16().lowerCase().encode(DIRECTORY_MD5);
   }

   @Override
   public String putBlob(final String containerName, final Blob blob) throws IOException {
      String blobKey = blob.getMetadata().getName();
      Payload payload = blob.getPayload();

      InputStream payloadStream = payload.openStream();

      HashingInputStream his = new HashingInputStream(Hashing.md5(), payloadStream);
      // Reset input stream back to beginning
      payloadStream.reset();

      kineticContainerNameValidator.validate(containerName);
      kineticBlobKeyValidator.validate(blobKey);
      if (getDirectoryBlobSuffix(blobKey) != null) {
         return putDirectoryBlob(containerName, blob);
      }

      long fileLength = payload.getContentMetadata().getContentLength();
      long chunksRequired = numberOfChunksForSize(fileLength);
      int chunkDataLength = KineticConstants.PROPERTY_CHUNK_SIZE_BYTES - KineticConstants
              .PROPERTY_CHUNK_FULL_HEADER_SIZE_BYTES;
      int currentChunk = 0;
      long fileId = -1;
      try {
          fileId = KineticDatabaseUtils.getInstance().getFileIdFromDatabase(containerName + "/" + blobKey);
      } catch (SQLException sqle) {
          sqle.printStackTrace();
      }
      while (currentChunk < chunksRequired) {
          Chunk chunk = new Chunk(this, fileId, currentChunk);
          byte[] chunkData = new byte[KineticConstants.PROPERTY_CHUNK_SIZE_BYTES];

          // Get header type values
          Map<String, String> headers = getChunkHeaders(containerName, blobKey, currentChunk);
          String chunkKey = getChunkKey(containerName, blobKey, currentChunk);

          // Set header values into the actual data of the chunk
          byte[] headerBytes = chunkKey.getBytes("UTF-8");
          for (int i = 0; i < headerBytes.length; i++) {
              chunkData[i] = headerBytes[i];
          }

          // Read data from blob into chunk
          payload.openStream().read(chunkData, headerBytes.length, chunkDataLength);
          chunk.setData(chunkData);

          // Send data to KDCC
          try {
              KineticDatabaseUtils.getInstance().addChunkToDatabase(chunkKey, chunkData);
          } catch (SQLException sqle) {
              return null;
          }

      }
       try {
           KineticDatabaseUtils.getInstance().addFileToDatabase(containerName + "/" + blobKey, fileLength);
       } catch (SQLException e) {
           e.printStackTrace();
       }

       if (payload != null) {
             payload.release();
         }
       return base16().lowerCase().encode(his.hash().asBytes());
   }

   @Override
   public void removeBlob(final String container, final String blobKey) {
      kineticContainerNameValidator.validate(container);
      kineticBlobKeyValidator.validate(blobKey);
      String fileName = buildPathStartingFromBaseDir(container, blobKey);
      logger.debug("Deleting blob %s", fileName);
      File fileToBeDeleted = new File(fileName);

      if (fileToBeDeleted.isDirectory()) {
         try {
            UserDefinedFileAttributeView view = getUserDefinedFileAttributeView(fileToBeDeleted.toPath());
            if (view != null) {
               for (String s : view.list()) {
                  view.delete(s);
               }
            }
         } catch (IOException e) {
            logger.debug("Could not delete attributes from %s: %s", fileToBeDeleted, e);
         }
      }

      try {
         delete(fileToBeDeleted);
      } catch (IOException e) {
         logger.debug("Could not delete %s: %s", fileToBeDeleted, e);
      }

      // now examine if the key of the blob is a complex key (with a directory structure)
      // and eventually remove empty directory
      removeDirectoriesTreeOfBlobKey(container, blobKey);
   }

   @Override
   public BlobAccess getBlobAccess(String containerName, String blobName) {
      Path path = new File(buildPathStartingFromBaseDir(containerName, blobName)).toPath();

      if ( isWindows() ) {
         try {
            if (isPrivate(path)) {
               return BlobAccess.PRIVATE;
            } else {
               return BlobAccess.PUBLIC_READ;
            }
         } catch (IOException e) {
            throw new RuntimeException(e);
         }
      } else {
         Set<PosixFilePermission> permissions;
         try {
            permissions = getPosixFilePermissions(path);
         } catch (IOException ioe) {
            throw Throwables.propagate(ioe);
         }
         return permissions.contains(PosixFilePermission.OTHERS_READ)
               ? BlobAccess.PUBLIC_READ : BlobAccess.PRIVATE;
      }
   }

   @Override
   public void setBlobAccess(String container, String name, BlobAccess access) {
      Path path = new File(buildPathStartingFromBaseDir(container, name)).toPath();
      if ( isWindows() ) {
         try {
            if (access == BlobAccess.PRIVATE) {
               setPrivate(path);
            } else {
               setPublic(path);
            }
         } catch (IOException e) {
            throw new RuntimeException(e);
         }
      } else {
         Set<PosixFilePermission> permissions;
         try {
            permissions = getPosixFilePermissions(path);
            if (access == BlobAccess.PRIVATE) {
               permissions.remove(PosixFilePermission.OTHERS_READ);
            } else if (access == BlobAccess.PUBLIC_READ) {
               permissions.add(PosixFilePermission.OTHERS_READ);
            }
            setPosixFilePermissions(path, permissions);
         } catch (IOException ioe) {
            throw Throwables.propagate(ioe);
         }
      }
   }

   @Override
   public Location getLocation(final String containerName) {
      return defaultLocation.get();
   }

   @Override
   public String getSeparator() {
      return File.separator;
   }

   public boolean createContainer(String container) {
      kineticContainerNameValidator.validate(container);
      return createContainerInLocation(container, null, CreateContainerOptions.NONE);
   }

   public Blob newBlob(@ParamValidators({ KineticBlobKeyValidator.class }) String name) {
      kineticBlobKeyValidator.validate(name);
      return blobBuilders.get().name(name).build();
   }

   /**
    * Returns a {@link File} object that links to the blob
    *
    * @param container
    * @param blobKey
    * @return
    */
   public File getFileForBlobKey(String container, String blobKey) {
      kineticContainerNameValidator.validate(container);
      kineticBlobKeyValidator.validate(blobKey);
      String fileName = buildPathStartingFromBaseDir(container, blobKey);
      File blobFile = new File(fileName);
      return blobFile;
   }

   public boolean directoryExists(String container, String directory) {
      return buildPathAndChecksIfDirectoryExists(container, directory);
   }

   public void createDirectory(String container, String directory) {
      createDirectoryWithResult(container, directory);
   }

   public void deleteDirectory(String container, String directory) {
      // create complete dir path
      String fullDirPath = buildPathStartingFromBaseDir(container, directory);
      try {
         Utils.deleteRecursively(new File(fullDirPath));
      } catch (IOException ex) {
         logger.error("An error occurred removing directory %s.", fullDirPath);
         Throwables.propagate(ex);
      }
   }

   public long countBlobs(String container, ListContainerOptions options) {
      // TODO: honor options
      try {
         return Iterables.size(getBlobKeysInsideContainer(container));
      } catch (IOException ioe) {
         throw Throwables.propagate(ioe);
      }
   }

   // ---------------------------------------------------------- Private methods

   private boolean buildPathAndChecksIfBlobExists(String... tokens) throws IOException {
      String path = buildPathStartingFromBaseDir(tokens);
      File file = new File(path);
      boolean exists = file.exists() && file.isFile();
      if (!exists && getDirectoryBlobSuffix(tokens[tokens.length - 1]) != null
              && file.isDirectory()) {
         UserDefinedFileAttributeView view = getUserDefinedFileAttributeView(file.toPath());
         exists = view != null && view.list().contains(XATTR_CONTENT_MD5);
      }
      return exists;
   }

   private static String getDirectoryBlobSuffix(String key) {
      for (String suffix : BlobStoreConstants.DIRECTORY_SUFFIXES) {
         if (key.endsWith(suffix)) {
            return suffix;
         }
      }
      return null;
   }

   private static String directoryBlobName(String key) {
      String suffix = getDirectoryBlobSuffix(key);
      if (suffix != null) {
         if (!BlobStoreConstants.DIRECTORY_BLOB_SUFFIX.equals(suffix)) {
            key = key.substring(0, key.lastIndexOf(suffix));
         }
         return key + BlobStoreConstants.DIRECTORY_BLOB_SUFFIX;
      }
      return null;
   }

   private UserDefinedFileAttributeView getUserDefinedFileAttributeView(Path path) throws IOException {
      return getFileAttributeView(path, UserDefinedFileAttributeView.class);
   }

   /**
    * Check if the file system resource whose name is obtained applying buildPath on the input path
    * tokens is a directory, otherwise a RuntimeException is thrown
    *
    * @param tokens
    *           the tokens that make up the name of the resource on the file system
    */
   private boolean buildPathAndChecksIfDirectoryExists(String... tokens) {
      String path = buildPathStartingFromBaseDir(tokens);
      File file = new File(path);
      boolean exists = file.exists() || file.isDirectory();
      return exists;
   }

   /**
    * Facility method used to concatenate path tokens normalizing separators
    *
    * @param pathTokens
    *           all the string in the proper order that must be concatenated in order to obtain the
    *           filename
    * @return the resulting string
    */
   protected String buildPathStartingFromBaseDir(String... pathTokens) {
      String normalizedToken = removeFileSeparatorFromBorders(normalize(baseDirectory), true);
      StringBuilder completePath = new StringBuilder(normalizedToken);
      if (pathTokens != null && pathTokens.length > 0) {
         for (int i = 0; i < pathTokens.length; i++) {
            if (pathTokens[i] != null) {
               normalizedToken = removeFileSeparatorFromBorders(normalize(pathTokens[i]), false);
               completePath.append(File.separator).append(normalizedToken);
            }
         }
      }
      return completePath.toString();
   }

   /**
    * Substitutes all the file separator occurrences in the path with a file separator for the
    * current operative system
    *
    * @param pathToBeNormalized
    * @return
    */
   private static String normalize(String pathToBeNormalized) {
      if (null != pathToBeNormalized && pathToBeNormalized.contains(BACK_SLASH)) {
         if (!BACK_SLASH.equals(File.separator)) {
            return pathToBeNormalized.replace(BACK_SLASH, File.separator);
         }
      }
      return pathToBeNormalized;
   }

   private static String denormalize(String pathToDenormalize) {
      if (null != pathToDenormalize && pathToDenormalize.contains("/")) {
         if (BACK_SLASH.equals(File.separator)) {
              return pathToDenormalize.replace("/", BACK_SLASH);
         }
      }
      return pathToDenormalize;
   }

   /**
    * Remove leading and trailing separator character from the string.
    *
    * @param pathToBeCleaned
    * @param onlyTrailing
    *           only trailing separator char from path
    * @return
    */
   private String removeFileSeparatorFromBorders(String pathToBeCleaned, boolean onlyTrailing) {
      if (null == pathToBeCleaned || pathToBeCleaned.equals(""))
         return pathToBeCleaned;

      int beginIndex = 0;
      int endIndex = pathToBeCleaned.length();

      // search for separator chars
      if (!onlyTrailing) {
         if (pathToBeCleaned.substring(0, 1).equals(File.separator))
            beginIndex = 1;
      }
      if (pathToBeCleaned.substring(pathToBeCleaned.length() - 1).equals(File.separator))
         endIndex--;

      return pathToBeCleaned.substring(beginIndex, endIndex);
   }

   /**
    * Removes recursively the directory structure of a complex blob key, only if the directory is
    * empty
    *
    * @param container
    * @param blobKey
    */
   private void removeDirectoriesTreeOfBlobKey(String container, String blobKey) {
      String normalizedBlobKey = denormalize(blobKey);
      // exists is no path is present in the blobkey
      if (!normalizedBlobKey.contains(File.separator))
         return;

      File file = new File(normalizedBlobKey);
      // TODO
      // "/media/data/works/java/amazon/jclouds/master/kinetic/aa/bb/cc/dd/eef6f0c8-0206-460b-8870-352e6019893c.txt"
      String parentPath = file.getParent();
      // no need to manage "/" parentPath, because "/" cannot be used as start
      // char of blobkey
      if (!isNullOrEmpty(parentPath)) {
         // remove parent directory only it's empty
         File directory = new File(buildPathStartingFromBaseDir(container, parentPath));
         // don't delete directory if it's a directory blob
         try {
            UserDefinedFileAttributeView view = getUserDefinedFileAttributeView(directory.toPath());
            if (view == null) { // OSX HFS+ does not support UserDefinedFileAttributeView
                logger.debug("Could not look for attributes from %s", directory);
            } else if (!view.list().isEmpty()) {
               return;
            }
         } catch (IOException e) {
            logger.debug("Could not look for attributes from %s: %s", directory, e);
         }

         String[] children = directory.list();
         if (null == children || children.length == 0) {
            try {
               delete(directory);
            } catch (IOException e) {
               logger.debug("Could not delete %s: %s", directory, e);
               return;
            }
            // recursively call for removing other path
            removeDirectoriesTreeOfBlobKey(container, parentPath);
         }
      }
   }

   private File openFolder(String folderName) throws IOException {
      String baseFolderName = buildPathStartingFromBaseDir(folderName);
      File folder = new File(baseFolderName);
      if (folder.exists()) {
         if (!folder.isDirectory()) {
            throw new IOException("Resource " + baseFolderName + " isn't a folder.");
         }
      }
      return folder;
   }

   private static void populateBlobKeysInContainer(File directory, Set<String> blobNames,
         Function<String, String> function) {
      File[] children = directory.listFiles();
      if (children == null) {
         return;
      }
      for (File child : children) {
         if (child.isFile()) {
            blobNames.add( function.apply(child.getAbsolutePath()) );
         } else if (child.isDirectory()) {
            blobNames.add(function.apply(child.getAbsolutePath()) + File.separator); // TODO: undo if failures
            populateBlobKeysInContainer(child, blobNames, function);
         }
      }
   }

   /**
    * Creates a directory and returns the result
    *
    * @param container
    * @param directory
    * @return true if the directory was created, otherwise false
    */
   protected boolean createDirectoryWithResult(String container, String directory) {
      String directoryFullName = buildPathStartingFromBaseDir(container, directory);
      logger.debug("Creating directory %s", directoryFullName);

      // cannot use directoryFullName, because the following method rebuild
      // another time the path starting from base directory
      if (buildPathAndChecksIfDirectoryExists(container, directory)) {
         logger.debug("Directory %s already exists", directoryFullName);
         return false;
      }

      File directoryToCreate = new File(directoryFullName);
      boolean result = directoryToCreate.mkdirs();
      return result;
   }

   /** Read the String representation of kinetic attribute, or return null if not present. */
   private static String readStringAttributeIfPresent(UserDefinedFileAttributeView view, Set<String> attributes,
         String name) throws IOException {
      if (!attributes.contains(name)) {
         return null;
      }
      ByteBuffer buf = ByteBuffer.allocate(view.size(name));
      view.read(name, buf);
      return new String(buf.array(), StandardCharsets.UTF_8);
   }

   /** Write an kinetic attribute, if its value is non-null. */
   private static void writeStringAttributeIfPresent(UserDefinedFileAttributeView view, String name, String value) throws IOException {
      if (value != null) {
         view.write(name, ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8)));
      }
   }

   private static void copyStringAttributeIfPresent(UserDefinedFileAttributeView view, String name, Map<String, String> attrs) throws IOException {
      writeStringAttributeIfPresent(view, name, attrs.get(name));
   }
}