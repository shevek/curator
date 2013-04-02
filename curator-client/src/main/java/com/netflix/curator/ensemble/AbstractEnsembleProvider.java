/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.netflix.curator.ensemble;

import java.util.concurrent.CopyOnWriteArrayList;

/**
 *
 * @author shevek
 */
public abstract class AbstractEnsembleProvider implements EnsembleProvider {

    private final CopyOnWriteArrayList<EnsembleListener> listeners = new CopyOnWriteArrayList<EnsembleListener>();

    @Override
    public void addListener(EnsembleListener listener) {
        listeners.addIfAbsent(listener);
    }

    protected void fireEnsembleChanged() {
        for (EnsembleListener listener : listeners)
            listener.ensembleChanged(AbstractEnsembleProvider.this);
    }
}
