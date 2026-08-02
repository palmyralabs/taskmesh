package com.palmyralabs.taskmesh.starter;

import com.palmyralabs.taskmesh.api.PayloadProvider;
import com.palmyralabs.taskmesh.api.PayloadProviderFactory;

/** Auto-configured default when exactly one {@link PayloadProvider} bean exists — every task_type resolves to it. */
public class SingleProviderPayloadProviderFactory implements PayloadProviderFactory {

    private final PayloadProvider provider;

    public SingleProviderPayloadProviderFactory(PayloadProvider provider) {
        this.provider = provider;
    }

    @Override
    public PayloadProvider getProvider(String taskType) {
        return provider;
    }
}
