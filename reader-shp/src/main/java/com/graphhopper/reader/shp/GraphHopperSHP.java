/*
 *  Licensed to GraphHopper GmbH under one or more contributor
 *  license agreements. See the NOTICE file distributed with this work for 
 *  additional information regarding copyright ownership.
 * 
 *  GraphHopper GmbH licenses this file to you under the Apache License, 
 *  Version 2.0 (the "License"); you may not use this file except in 
 *  compliance with the License. You may obtain a copy of the License at
 * 
 *       http://www.apache.org/licenses/LICENSE-2.0
 * 
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.graphhopper.reader.shp;

import java.util.HashSet;

import com.graphhopper.GraphHopper;
import com.graphhopper.reader.DataReader;
import com.graphhopper.storage.GraphHopperStorage;
import com.graphhopper.util.CmdArgs;

/**
 * This class is the main entry point to import from OpenStreetMap shape files similar to GraphHopperOSM which imports
 * OSM xml and pbf files.
 *
 * @author Phil
 */
public class GraphHopperSHP extends GraphHopper {
    private final HashSet<EdgeAddedListener> edgeAddedListeners = new HashSet<>();

    private final int shapeFileType;
    private final String speedData;
    private final String period;

    public GraphHopperSHP(int shapeType, CmdArgs args) {
        shapeFileType = shapeType;
        speedData = args.get("speeds.file", null);
        period = args.get("speeds.period", null);
    }

    @Override
    protected DataReader createReader(GraphHopperStorage ghStorage) {
        ShapeFileReader reader;
        if (shapeFileType == 0)
            reader = new OSMShapeFileReader(ghStorage, speedData, period);
        else
            reader = new ITNShapeFileReader(ghStorage, speedData, period);
        for (EdgeAddedListener l : edgeAddedListeners) {
            reader.addListener(l);
        }
        return initDataReader(reader);
    }

    public void addListener(EdgeAddedListener l) {
        edgeAddedListeners.add(l);
    }

}
