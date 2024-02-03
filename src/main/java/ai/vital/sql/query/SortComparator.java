package ai.vital.sql.query;

import java.util.Comparator;
import java.util.List;

import ai.vital.vitalservice.query.VitalGraphQueryPropertyCriterion;
import ai.vital.vitalservice.query.VitalSortProperty;
import ai.vital.vitalsigns.ontology.VitalCoreOntology;

public class SortComparator implements Comparator<URIResultElement> {

	private List<VitalSortProperty> sortProperties;

	public SortComparator(List<VitalSortProperty> sortProperties) {
		this.sortProperties = sortProperties;
	}

	@Override
	public int compare(URIResultElement e1, URIResultElement e2) {

		for(int i = 0 ; i < sortProperties.size(); i ++) {
			
			VitalSortProperty sp = sortProperties.get(i);
			
			boolean reverse = sp.isReverse();
			
			String propertyURI = sp.getPropertyURI();
			
			Comparable v1 = null;
			Comparable v2 = null;
			
			if(propertyURI.equals(VitalGraphQueryPropertyCriterion.URI) || propertyURI.equals(VitalCoreOntology.URIProp.getURI())) {
				
				v1 = e1.URI;
				v2 = e2.URI;
				
			} else {
				
				v1 = (Comparable) (e1.attributes != null ? e1.attributes.get(propertyURI) : null);
				v2 = (Comparable) (e2.attributes != null ? e2.attributes.get(propertyURI) : null);
 				
			}
			
			if(v1 == null && v2 == null) {
				continue;
			} else if(v1 == null){
				
				return reverse ? 1 : -1;
				
			} else if(v2 == null) {
				
				return reverse ? -1 : 1;
				
			}
			
			int c = reverse ? v2.compareTo(v1) : v1.compareTo(v2);
			
			if(c != 0) return c;
			
		}
		
		return 0;
	}

}
