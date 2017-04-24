package io.realm;

public abstract class RealmInstanceCallback<T extends BaseRealm> {
    public abstract void onSuccess(T realm);

    public void onError(RuntimeException exception) {
        throw exception;
    }
}
