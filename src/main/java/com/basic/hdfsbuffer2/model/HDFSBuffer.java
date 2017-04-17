package com.basic.hdfsbuffer2.model;

import java.nio.ByteBuffer;

/**
 * locate com.basic.hdfsbuffer2.model
 * Created by 79875 on 2017/4/17.
 */
public class HDFSBuffer {
    public ByteBuffer byteBuffer;
    private boolean isBufferFinished=false;//是否缓存完毕，初始化缓存没完成

    public HDFSBuffer(ByteBuffer byteBuffer, boolean isBufferFinished) {
        this.byteBuffer = byteBuffer;
        this.isBufferFinished = isBufferFinished;
    }

    public HDFSBuffer() {

    }

    public ByteBuffer getByteBuffer() {
        return byteBuffer;
    }

    public void setByteBuffer(ByteBuffer byteBuffer) {
        this.byteBuffer = byteBuffer;
    }

    public boolean isBufferFinished() {
        return isBufferFinished;
    }

    public void setBufferFinished(boolean bufferFinished) {
        isBufferFinished = bufferFinished;
    }
}
