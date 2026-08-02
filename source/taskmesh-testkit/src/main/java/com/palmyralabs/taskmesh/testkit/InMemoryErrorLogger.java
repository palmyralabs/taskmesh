package com.palmyralabs.taskmesh.testkit;

import com.palmyralabs.taskmesh.api.ErrorLogger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class InMemoryErrorLogger implements ErrorLogger {

    public record LoggedError(String itemId, String message) {
    }

    private final List<LoggedError> errors = new ArrayList<>();

    @Override
    public void log(String itemId, String message) {
        errors.add(new LoggedError(itemId, message));
    }

    public List<LoggedError> errors() {
        return Collections.unmodifiableList(errors);
    }
}
