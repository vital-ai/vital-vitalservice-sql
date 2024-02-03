package ai.vital.sql.query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.hp.hpl.jena.graph.Graph;

import ai.vital.sql.model.VitalSignsToSqlBridge;
import ai.vital.vitalservice.query.QueryContainerType;
import ai.vital.vitalservice.query.VitalGraphCriteriaContainer;
import ai.vital.vitalservice.query.VitalGraphQueryElement;
import ai.vital.vitalservice.query.VitalGraphQueryPropertyCriterion;
import ai.vital.vitalservice.query.VitalGraphQueryPropertyCriterion.Comparator;
import ai.vital.vitalservice.query.VitalGraphQueryTypeCriterion;
import ai.vital.vitalsigns.VitalSigns;
import ai.vital.vitalsigns.classes.ClassMetadata;
import ai.vital.vitalsigns.model.GraphObject;
import ai.vital.vitalsigns.model.property.URIProperty;
import ai.vital.vitalsigns.ontology.VitalCoreOntology;

/**
 * This optimization now also includes timestamp and channel queries
 *
 */
public class VitalTypeANDConstraintOptimization {

	
	static class TypeANDResponse {
		
		List<String> types = null;
		
		boolean inValues = true;
		
		Long minTimestamp = null;
		boolean minTimestampInclusive = false;
		Long maxTimestamp = null;
		boolean maxTimestampInclusive = false;
		
		Set<String> channelURIs = null;
		Comparator channelURIComparator = null;
		
		List<String> warnings = new ArrayList<String>(); 
		
		List<VitalGraphQueryPropertyCriterion> coveredCriteria = null;

		public TypeANDResponse(List<String> types, boolean inValues, Long minTimestamp, boolean minTimestampInclusive, Long maxTimestamp, boolean maxTimestampInclusive, 
				Set<String> channelURIs, Comparator channelURIComparator, List<VitalGraphQueryPropertyCriterion> coveredCriteria, List<String> warnings) {
			super();
			this.types = types;
			this.inValues = inValues;
			this.minTimestamp = minTimestamp;
			this.minTimestampInclusive = minTimestampInclusive;
			this.maxTimestamp = maxTimestamp;
			this.maxTimestampInclusive = maxTimestampInclusive;
			this.channelURIs = channelURIs;
			this.channelURIComparator = channelURIComparator; 
			this.coveredCriteria = coveredCriteria;
			this.warnings = warnings;
		}
		

	}
	
	static String channelURIProperty = "http://vital.ai/ontology/vital-aimp#hasChannelURI";
	
	static boolean isChannelURIProperty(VitalGraphQueryPropertyCriterion pc) {
		return channelURIProperty.equals(pc.getPropertyURI()); 
		
	}
	
	static boolean isTimestampURIProperty(VitalGraphQueryPropertyCriterion pc) {
		return VitalCoreOntology.hasTimestamp.getURI().equals(pc.getPropertyURI());
	}
	
	/**
	 * This function determines if a container applies
	 * @return
	 */
	static TypeANDResponse isVitalTypePropertyANDContainer(VitalGraphCriteriaContainer container) {

		List<VitalGraphQueryPropertyCriterion> coveredCriteria = new ArrayList<VitalGraphQueryPropertyCriterion>();
		
		List<String> warnings = new ArrayList<String>(); 
		
		Comparator channelURIComparator = null;
		
		Long minTimestamp = null;
		boolean minTimestampInclusive = false;
		Long maxTimestamp = null;
		boolean maxTimestampInclusive = false;
		
		Set<String> channelURIs = new HashSet<String>();
		
		if(container.getType() != QueryContainerType.and) return null;
		
		List<VitalGraphQueryTypeCriterion> vitaltypeconstraints = new ArrayList<VitalGraphQueryTypeCriterion>();
		
		List<VitalGraphQueryPropertyCriterion> propertiesConstraints = new ArrayList<VitalGraphQueryPropertyCriterion>();
		
		for(VitalGraphQueryElement el : container ) {
			
			if(el instanceof VitalGraphCriteriaContainer) return null;
			
			if(el instanceof VitalGraphQueryTypeCriterion) {
				
				vitaltypeconstraints.add((VitalGraphQueryTypeCriterion) el);
				
			} else if(el instanceof VitalGraphQueryPropertyCriterion) {

				VitalGraphQueryPropertyCriterion pc = (VitalGraphQueryPropertyCriterion) el;
				
				if(isChannelURIProperty(pc)) {
					
					if(channelURIComparator != null) {
						
						warnings.add("more than one channel uri criterion");
						
					} else {

						//TODO complete comparator/value validation
						Comparator comparator = pc.getComparator();
						if(comparator == Comparator.EQ || comparator == Comparator.NE 
//								||comparator == Comparator.EXISTS || comparator == Comparator.NOT_EXISTS 
								|| comparator == Comparator.ONE_OF || comparator == Comparator.NONE_OF) {
							
							Object value = pc.getValue();
							if(value instanceof Collection) {
								for(Object x : (Collection)value) {
									if(x instanceof URIProperty || value instanceof String) {
										channelURIs.add(x.toString());
									}
								}
							} else if(value instanceof URIProperty || value instanceof String){
								channelURIs.add(value.toString());
							} else if(value == null) {
								//ok
							}
							channelURIComparator = comparator;
							coveredCriteria.add(pc);
							continue;
						} else {
							warnings.add("channelURI comparator unsupported:" + comparator);
						}
						
					}
					
				} else if(isTimestampURIProperty(pc)) {
					
					Comparator c = pc.getComparator();

					boolean accepted = false;
					
					if(c == Comparator.GE || c == Comparator.GT) {
						if(minTimestamp == null) {
							minTimestampInclusive = c == Comparator.GE;
							minTimestamp = ((Number)pc.getValue()).longValue();
							accepted = true;							
						} else {
							warnings.add("more than one mintimestamp criterion");
						}

					} else if(c == Comparator.LE || c == Comparator.LT) {
						if(maxTimestamp == null) {
							maxTimestampInclusive = c == Comparator.LE;
							maxTimestamp = ((Number)pc.getValue()).longValue();
							accepted = true;
						} else {
							warnings.add("more than one maxtimestamp criterion");
						}
						
					} else {
						warnings.add("only range timestamp properties are optimized at this moment: " + c);
					}
					
					if(accepted) {
						coveredCriteria.add(pc);
						continue;
					}
					
				}
				
				propertiesConstraints.add((VitalGraphQueryPropertyCriterion) el);
				
				
				
			} else {
				throw new RuntimeException("Unexpected query element type: " + el);
			}
			
		}
		
		
		if( ( vitaltypeconstraints.size() > 0 || coveredCriteria.size() > 0 ) ) {
			
			Boolean inValues = null;
			
			Set<String> typeURIs = new HashSet<String>();
			
			for(VitalGraphQueryTypeCriterion tc : vitaltypeconstraints) {

				boolean newVal = tc.getComparator().equals(Comparator.EQ) || tc.getComparator().equals(Comparator.ONE_OF);
				
				if(inValues == null) {
					inValues = newVal;
				} else {
					if(newVal != inValues.booleanValue()) {
						throw new RuntimeException("Mixed type constraint comparator in a container detected");
					}
				}
				
				List<ClassMetadata> subclasses = new ArrayList<ClassMetadata>();

				Class<? extends GraphObject> gType = tc.getType();
				
				if(gType == null) {
					
					//special case for classes as uris list
					List types = (List) tc.getValue();
					for(Object t : types) {
						if(t instanceof Class) {
							String u = VitalSigns.get().getClassesRegistry().getClassURI((Class<? extends GraphObject>) t);
							if(u == null) throw new RuntimeException("Class not found: " + t);
							typeURIs.add(u);
						} else if(t instanceof URIProperty) {
							typeURIs.add(((URIProperty)t).get());
						} else if(t instanceof String) {
							typeURIs.add((String) t);
						} else {
							throw new RuntimeException("Unknown type criterion value list element " + t );
						}
					}
					
					if(typeURIs.size() == 0) throw new RuntimeException("Empty type uris input set");
					
				} else {
					
					
					ClassMetadata cm = VitalSigns.get().getClassesRegistry().getClassMetadata(gType);
					if(cm == null) throw new RuntimeException("Class metadata not found for type: " + gType.getCanonicalName());
					
					if(tc.isExpandTypes()) {
						
						subclasses = VitalSigns.get().getClassesRegistry().getSubclasses(cm, true);
						
					} else {
						
						subclasses.add(cm);
						
					}
					
					for(ClassMetadata sc : subclasses) {
						
						typeURIs.add(sc.getURI());
						
						
					}
					
				}
				
			}
			
			if(inValues == null) inValues = false;
			
			return new TypeANDResponse(new ArrayList<String>(typeURIs),  inValues.booleanValue(), minTimestamp, minTimestampInclusive, maxTimestamp, maxTimestampInclusive, channelURIs, channelURIComparator, coveredCriteria, warnings);
			
		}
		
		return null;
		
		
	}

	/*
	public static VitalTypeANDFilterExpressionResponse createFilterExpression(
			VitalGraphCriteriaContainer queryContainer) {

		boolean first = true;
		
		StringBuilder expression = new StringBuilder();
		
		Map<String, String> expressionAttributeNames = new HashMap<String, String>();
		
		Map<String, AttributeValue> expressionAttributeValues = new HashMap<String, AttributeValue>();
		
		for(VitalGraphQueryElement el : queryContainer) {
			
			if(el instanceof VitalGraphQueryTypeCriterion) {
				VitalGraphQueryTypeCriterion tc = (VitalGraphQueryTypeCriterion) el;
				
				if(first) {
					first = false;
				} else {
					expression.append("AND\n");
				}

				List<ClassMetadata> subclasses = new ArrayList<ClassMetadata>();

				Class<? extends GraphObject> gType = tc.getType();
				
				if(gType == null) throw new RuntimeException("No class set in type criterion" + tc);
				
				ClassMetadata cm = VitalSigns.get().getClassesRegistry().getClassMetadata(gType);
				if(cm == null) throw new RuntimeException("Class metadata not found for type: " + gType.getCanonicalName());
				
				if(tc.isExpandTypes()) {
					
					subclasses = VitalSigns.get().getClassesRegistry().getSubclasses(cm, true);
					
				} else {
					
					subclasses.add(cm);
					
				}
				
				
				if(subclasses.size() > 1) {
					
					expression.append("( ");
					
				}
				
				for(int i = 0 ; i < subclasses.size(); i++) {
					
					String typeURI = subclasses.get(i).getURI();
					
					if(i > 0) {
						
						expression.append(" OR ");
						
					}
					
					String name = ":val" + expressionAttributeValues.size();
					
					expression.append(CoreOperations.vitaltype + " = " + name);
					
					expressionAttributeValues.put(name, new AttributeValue().withS(typeURI));
					
					
				}
				
				if(subclasses.size() > 1) {
					
					expression.append(" )");
					
				}
				
				expression.append("\n");
				
			} else if(el instanceof VitalGraphQueryPropertyCriterion) {
				
			} else {
				throw new RuntimeException("Unexpected element here: " + el);
			}
			
		}
		
		return new VitalTypeANDFilterExpressionResponse(expression.toString(), expressionAttributeNames, expressionAttributeValues);
	}
	*/
	
}
