package ai.vital.sql.query;

import java.util.ArrayList;
import java.util.List;

import ai.vital.vitalservice.query.QueryContainerType;
import ai.vital.vitalservice.query.VitalGraphCriteriaContainer;
import ai.vital.vitalservice.query.VitalGraphQueryElement;
import ai.vital.vitalservice.query.VitalGraphQueryTypeCriterion;
import ai.vital.vitalservice.query.VitalGraphQueryPropertyCriterion.Comparator;
import ai.vital.vitalsigns.VitalSigns;
import ai.vital.vitalsigns.model.GraphObject;

public class NOT_EQUALTypeOptimization {

	public static List<String> notEqualTypeOptimization(VitalGraphCriteriaContainer container) {
		
		if(container.getType() != QueryContainerType.and) {
			return null;
		}
		
		List<String> types = null;
		
		for(VitalGraphQueryElement el : container) {
			
			if(el instanceof VitalGraphQueryTypeCriterion) {
				
				VitalGraphQueryTypeCriterion tc = (VitalGraphQueryTypeCriterion) el;
				
				if( ( tc.getComparator() == Comparator.NE && !tc.isNegative() ) || ( tc.getComparator() == Comparator.EQ && tc.isNegative()) ) {
					
					if(types == null) {
						types = new ArrayList<String>();
					}
					
					Class<? extends GraphObject> type = tc.getType();
					
					String classURI = VitalSigns.get().getClassesRegistry().getClassURI(type);
					
					if(classURI == null) throw new RuntimeException("No class URI found for type: " + type);
					 
					types.add(classURI);
					
				} else {
					
					return null;
					
				}
				
			} else {
				
				//everything else breaks the rule
				return null;
			}
			
		}
		
		return types;
		
	}
	
}
