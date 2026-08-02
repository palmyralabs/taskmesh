package com.palmyralabs.taskmesh.api;

public interface PayloadCodec {

    Object decode(byte[] raw);

    byte[] encode(Object payload);
}
