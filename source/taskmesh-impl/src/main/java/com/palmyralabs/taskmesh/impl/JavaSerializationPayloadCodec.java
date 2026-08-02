package com.palmyralabs.taskmesh.impl;

import com.palmyralabs.taskmesh.api.PayloadCodec;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UncheckedIOException;

/**
 * JDK-serialization-based default. Type info travels inside the stream
 * itself, so unlike a plain JSON codec this needs no per-task_type class
 * binding to decode — a real fit for {@code PayloadCodec.decode(byte[])}'s
 * "no type hint" shape. A host wanting JSON-at-rest can supply its own
 * PayloadCodec (typically bound per task_type, since JSON needs a target
 * class); nothing here requires this one.
 */
public class JavaSerializationPayloadCodec implements PayloadCodec {

    @Override
    public Object decode(byte[] raw) {
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(raw))) {
            return in.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new UncheckedIOException("Failed to decode payload", e instanceof IOException io ? io : new IOException(e));
        }
    }

    @Override
    public byte[] encode(Object payload) {
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
             ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(payload);
            out.flush();
            return bytes.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to encode payload", e);
        }
    }
}
