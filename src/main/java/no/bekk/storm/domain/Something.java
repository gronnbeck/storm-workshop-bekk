package no.bekk.storm.domain;

import java.util.List;

/**
 * Created by steffen stenersen on 20/04/14.
 */
public interface Something<T> {
    public int getIndex();
    public T[] values();
}
