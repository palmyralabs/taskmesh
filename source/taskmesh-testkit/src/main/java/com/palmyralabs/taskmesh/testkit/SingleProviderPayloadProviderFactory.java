package com.palmyralabs.taskmesh.testkit;

import com.palmyralabs.taskmesh.api.PayloadProvider;
import com.palmyralabs.taskmesh.api.PayloadProviderFactory;
import lombok.RequiredArgsConstructor;

/** Returns the same {@link PayloadProvider} for every task_type — fine for tests exercising one or two types. */
@RequiredArgsConstructor
public class SingleProviderPayloadProviderFactory implements PayloadProviderFactory {

    private final PayloadProvider provider;

    @Override
    public PayloadProvider getProvider(String taskType) {
        return provider;
    }
}
