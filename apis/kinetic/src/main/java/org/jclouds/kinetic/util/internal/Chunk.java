package org.jclouds.kinetic.util.internal;

/**
 * Created by steph.zylstra on 10/02/2016.
 */
public class Chunk {

    Object metadata;

    public Chunk() {
        System.out.println(this.hashCode());
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
