package com.palmyralabs.taskmesh.testkit;

import com.palmyralabs.taskmesh.api.PayloadCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UncheckedIOException;

/**
 * JDK-serialization-based codec — testkit-only convenience so tests don't
 * need a JSON dependency to exercise Phase 3's decode step. A real consumer
 * would typically use a JSON codec (Jackson, etc.); that's a
 * spring-boot-starter/consumer choice, not core -engine's to make. Requires
 * payload objects to be {@link java.io.Serializable}.
 */
public class JavaSerializationPayloadCodec implements PayloadCodec {

    private static final Logger LOG = LoggerFactory.getLogger(JavaSerializationPayloadCodec.class);

    @Override
    public Object decode(byte[] raw) {
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(raw))) {
            return in.readObject();
        } catch (IOException | ClassNotFoundException e) {
            LOG.error("Failed to decode payload", e);
            throw new UncheckedIOException("Failed to decode payload", wrapIfNeeded(e));
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

    private static IOException wrapIfNeeded(Exception e) {
        return e instanceof IOException io ? io : new IOException(e);
    }
}
