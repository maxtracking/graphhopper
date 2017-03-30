package com.graphhopper.reader.shp;

import com.graphhopper.reader.ReaderWay;
import com.graphhopper.util.EdgeIteratorState;

/**
 * Created by alexeygusev on 27/03/2017.
 */
public interface EdgeAddedListener {
    void edgeAdded(ReaderWay way, EdgeIteratorState edge);
}
