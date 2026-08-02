package com.palmyralabs.taskmesh.engine.impl;

import com.palmyralabs.taskmesh.api.NodeInfoProvider;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * hostname + PID — stable for the process's lifetime, no configuration
 * required. The application never needs to invent a node id itself unless
 * it wants a more specific one (e.g. a k8s pod name from an env var).
 */
public class DefaultNodeInfoProvider implements NodeInfoProvider {

    private final String nodeId;

    public DefaultNodeInfoProvider() {
        String host;
        try {
            host = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            host = "unknown-host";
        }
        this.nodeId = host + "-" + ProcessHandle.current().pid();
    }

    @Override
    public String nodeId() {
        return nodeId;
    }
}
