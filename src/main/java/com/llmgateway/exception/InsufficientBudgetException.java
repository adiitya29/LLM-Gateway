package com.llmgateway.exception;

public class InsufficientBudgetException extends RuntimeException {

    private final long budgetMicroUsd;
    private final long spentMicroUsd;

    public InsufficientBudgetException(String message, long budgetMicroUsd, long spentMicroUsd) {
        super(message);
        this.budgetMicroUsd = budgetMicroUsd;
        this.spentMicroUsd = spentMicroUsd;
    }

    public long getBudgetMicroUsd() {
        return budgetMicroUsd;
    }

    public long getSpentMicroUsd() {
        return spentMicroUsd;
    }
}

