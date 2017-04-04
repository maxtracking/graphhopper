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

import com.graphhopper.reader.DataReader;
import com.graphhopper.routing.util.EncodingManager;
import com.graphhopper.storage.Graph;
import com.graphhopper.storage.GraphHopperStorage;
import com.graphhopper.storage.GraphStorage;
import com.graphhopper.storage.NodeAccess;
import com.vividsolutions.jts.geom.Coordinate;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.FeatureSource;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * ShapeFileReader takes care of reading a shape file and writing it to a road network graph
 *
 * @author Vikas Veshishth
 * @author Philip Welch
 */
public abstract class ShapeFileReader implements DataReader {

    protected final HashSet<EdgeAddedListener> edgeAddedListeners = new HashSet<>();
    protected final String speedData;
    protected final HashMap<String, Double> speedMap = new HashMap<>();

    protected EncodingManager encodingManager;

    private static final Logger LOGGER = LoggerFactory.getLogger(ShapeFileReader.class);

    private final GraphStorage graphStorage;
    private final NodeAccess nodeAccess;
    protected final Graph graph;

    public ShapeFileReader(GraphHopperStorage ghStorage, String speedData, String period) {
        this.graphStorage = ghStorage;
        this.graph = ghStorage;
        this.nodeAccess = graph.getNodeAccess();
        this.encodingManager = ghStorage.getEncodingManager();
        this.speedData = speedData;

        if (this.speedData != null) {
            loadSpeedData(period);
//            loadSpeedData("OffPeak1016MonFri");
        }
    }

    @Override
    public void readGraph() {
        graphStorage.create(1000);
        processJunctions();
        processRoads();
    }

    public void addListener(EdgeAddedListener l) {
        edgeAddedListeners.add(l);
    }


    abstract void processJunctions();

    abstract void processRoads();

    protected FeatureIterator<SimpleFeature> getFeatureIterator(DataStore dataStore) {
        if (dataStore == null)
            throw new IllegalArgumentException("DataStore cannot be null for getFeatureIterator");

        try {
            String typeName = dataStore.getTypeNames()[0];
            FeatureSource<SimpleFeatureType, SimpleFeature> source = dataStore.getFeatureSource(typeName);
            Filter filter = Filter.INCLUDE;
            FeatureCollection<SimpleFeatureType, SimpleFeature> collection = source.getFeatures(filter);

            FeatureIterator<SimpleFeature> features = collection.features();
            return features;

        } catch (Exception e) {
            throw Utils.asUnchecked(e);
        }
    }

    protected DataStore openShapefileDataStore(File file) {
        try {
            Map<String, Object> map = new HashMap<String, Object>();
            map.put("url", file.toURI().toURL());
            DataStore ds = DataStoreFinder.getDataStore(map);
            if (ds == null)
                throw new IllegalArgumentException("Cannot find DataStore at " + file);
            return ds;

        } catch (Exception e) {
            throw Utils.asUnchecked(e);
        }
    }

    /*
     * Get longitude using the current long-lat order convention
     */
    protected double lng(Coordinate coordinate) {
        return coordinate.getOrdinate(0);
    }

    /*
	 * Get latitude using the current long-lat order convention
     */
    protected double lat(Coordinate coordinate) {
        return coordinate.getOrdinate(1);
    }

    protected void saveTowerPosition(int nodeId, Coordinate point) {
        nodeAccess.setNode(nodeId, lat(point), lng(point));
    }

    private void loadSpeedData(String columnName) {
        CSVParser parser = null;
        List<CSVRecord> list;
        try {
            parser = new CSVParser(new FileReader(speedData), CSVFormat.DEFAULT.withFirstRecordAsHeader());
            list = parser.getRecords();
//            list.sort((o1, o2) -> {
//                float lat1 = Float.parseFloat(o1.get(1));
//                float lat2 = Float.parseFloat(o2.get(1));
//                if (lat1 > lat2)
//                    return 1;
//                else if (lat1 < lat2)
//                    return -1;
//                else
//                    return 0;
//            });
            double factor = 1;//1.60934;
            // Get speeds and convert to KPH (x 1.60934)
            for( CSVRecord row : list ) {
                String id = row.get("RoadLinkId");
                double speed = Double.parseDouble(row.get(columnName)) * factor;
//                if (speedMap.get(id) != null) {
//                    LOGGER.info("Key found: " + id);
//                }
                speedMap.put(id.substring(4), speed);
            }
            LOGGER.info("Number of nodes in speed map : " + speedMap.size());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
