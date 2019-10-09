package com.jackniu.kafka.common.requests;

public enum TransactionResult  {
    ABORT(false), COMMIT(true);

    public final boolean id;

    TransactionResult(boolean id) {
        this.id = id;
    }

    public static TransactionResult forId(boolean id) {
        if (id) {
            return TransactionResult.COMMIT;
        }
        return TransactionResult.ABORT;
    }

}
