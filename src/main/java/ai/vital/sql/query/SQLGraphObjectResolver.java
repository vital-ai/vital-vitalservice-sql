package ai.vital.sql.query;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import ai.vital.sql.dao.CoreOperations;
import ai.vital.sql.model.SegmentTable;
import ai.vital.vitalservice.query.QueryStats;
import ai.vital.vitalsigns.model.GraphObject;
import ai.vital.vitalsigns.query.graph.GraphObjectResolver;
import ai.vital.vitalsigns.rdf.RDFUtils;

public class SQLGraphObjectResolver implements GraphObjectResolver {

	Connection connection;
	
	boolean disabled = false;
	
	//this is 
	Map<String, SegmentTable> uri2SegmentTable = new HashMap<String, SegmentTable>();
	Map<SegmentTable, Map<String, Set<String>>> segmentTable2uri2PropsMap = new HashMap<SegmentTable, Map<String, Set<String>>>();

	private QueryStats queryStats;
	
	public SQLGraphObjectResolver(Connection connection, boolean disabled, QueryStats queryStats) {
		this.connection = connection;
		this.disabled = disabled;
		this.queryStats = queryStats;
	}
	
	

	@Override
	public GraphObject resolveGraphObject(GraphObject graphObject) {
		Map<String, GraphObject> input = new HashMap<String, GraphObject>();
		input.put(graphObject.getURI(), graphObject);
		input = resolveGraphObjects(input);
		return input.get(graphObject.getURI());
	}

	public void putGraphObjectPropertyToGet(SegmentTable table, String graphObjectURI, String propertyURI) {
		
		if(disabled) return;
		
		uri2SegmentTable.put(graphObjectURI, table);
		
		Map<String, Set<String>> map = segmentTable2uri2PropsMap.get(table);
		Set<String> set = null;
		if(map == null) {
			map = new HashMap<String, Set<String>>();
			segmentTable2uri2PropsMap.put(table, map);
		} else {
			set = map.get(graphObjectURI);
		}
		
		if(set == null) {
			set = new HashSet<String>();
			map.put(graphObjectURI, set);
		}
		
		set.add(propertyURI);
		
	}
		
	
	@Override
	public Map<String, GraphObject> resolveGraphObjects(Map<String, GraphObject> m) {
		
		if(disabled) throw new RuntimeException("This resolver is disabled");
		
		if(uri2SegmentTable.size() == 0 ) return m;
		
		//resolve it in a batch
		
		Map<String, Map<String, String>> resolved = null;
		try {
			resolved = CoreOperations.resolveProperties(connection, segmentTable2uri2PropsMap, queryStats);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		
		for(Entry<String, GraphObject> entry : m.entrySet() ) {

			Map<String, String> newValues = resolved.get(entry.getKey());
			
//				//resolve it now
//				String shortName = RDFUtils.getPropertyShortName(gentry.getKey());
//
//				newValues.put(shortName, s);
//				
//			}
			
			if(newValues != null) {
				
				GraphObject graphObject = entry.getValue();
				
				for(Entry<String, String> nv : newValues.entrySet()) {

					String shortName = RDFUtils.getPropertyShortName(nv.getKey());
					
					graphObject.setProperty(shortName, nv.getValue());
					
				}
				
			}
			
		}
		
		return m;
		
	}

	@Override
	public boolean supportsBatchResolve() {
		return true;
	}

}
