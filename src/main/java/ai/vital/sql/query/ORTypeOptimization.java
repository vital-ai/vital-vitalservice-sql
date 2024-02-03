package ai.vital.sql.query;

import java.util.ArrayList;
import java.util.List;

import ai.vital.vitalservice.query.QueryContainerType;
import ai.vital.vitalservice.query.VitalGraphCriteriaContainer;
import ai.vital.vitalservice.query.VitalGraphQueryElement;
import ai.vital.vitalservice.query.VitalGraphQueryTypeCriterion;
import ai.vital.vitalsigns.VitalSigns;
import ai.vital.vitalsigns.model.GraphObject;
import ai.vital.vitalservice.query.VitalGraphQueryPropertyCriterion.Comparator;

/**
 * Converts an OR container of type constraints into a list of types
 *
 */
public class ORTypeOptimization {

	public static List<String> orTypeOptimization(VitalGraphCriteriaContainer container) {
		
		if(container.getType() != QueryContainerType.or ) return null;
		
		int misses = 0;
		
		List<String> typeURIs = new ArrayList<String>();
		
		for(VitalGraphQueryElement el : container) {
			
			if(el instanceof VitalGraphQueryTypeCriterion) {
			
				VitalGraphQueryTypeCriterion tc = (VitalGraphQueryTypeCriterion) el;
				
				if(tc.getComparator() == Comparator.EQ) {
					
					Class<? extends GraphObject> type = tc.getType();
					
					String classURI = VitalSigns.get().getClassesRegistry().getClassURI(type);
					
					if(classURI == null) throw new RuntimeException("No class URI found for type: " + type);
					 
					typeURIs.add(classURI);
					continue;
					
				}
				
			}
			
			misses++;
			
		}
		
		if(misses == 0 && typeURIs.size() >= 2) {
			
			return typeURIs;
			
		} else {
			
			return null;
		}
		
	}
	
}
