/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.netflix.curator.ensemble;

import java.util.EventListener;

/**
 *
 * @author shevek
 */
public interface EnsembleListener extends EventListener {

    public void ensembleChanged(EnsembleProvider provider);
}
