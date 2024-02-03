package ai.vital.sql.query;

import static ai.vital.sql.model.VitalSignsToSqlBridge.COLUMN_URI;
import static ai.vital.sql.model.VitalSignsToSqlBridge.COLUMN_TSTAMP;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.collections.buffer.PriorityBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ai.vital.sql.connector.VitalSqlDataSource;
import ai.vital.sql.dao.CoreOperations;
import ai.vital.sql.model.SegmentTable;
import ai.vital.sql.query.VitalTypeANDConstraintOptimization.TypeANDResponse;
import ai.vital.sql.utils.SQLUtils;
import ai.vital.vitalservice.VitalStatus;
import ai.vital.vitalservice.query.AggregationType;
import ai.vital.vitalservice.query.QueryContainerType;
import ai.vital.vitalservice.query.QueryStats;
import ai.vital.vitalservice.query.QueryTime;
import ai.vital.vitalservice.query.ResultElement;
import ai.vital.vitalservice.query.ResultList;
import ai.vital.vitalservice.query.VitalExportQuery;
import ai.vital.vitalservice.query.VitalGraphCriteriaContainer;
import ai.vital.vitalservice.query.VitalGraphQueryElement;
import ai.vital.vitalservice.query.VitalGraphQueryPropertyCriterion;
import ai.vital.vitalservice.query.VitalGraphQueryTypeCriterion;
import ai.vital.vitalservice.query.VitalSelectAggregationQuery;
import ai.vital.vitalservice.query.VitalSelectQuery;
import ai.vital.vitalservice.query.VitalSortProperty;
import ai.vital.vitalsigns.VitalSigns;
import ai.vital.vitalsigns.model.AggregationResult;
import ai.vital.vitalsigns.model.GraphMatch;
import ai.vital.vitalsigns.model.GraphObject;
import ai.vital.vitalsigns.model.VitalApp;
import ai.vital.vitalsigns.ontology.VitalCoreOntology;
import ai.vital.vitalsigns.properties.PropertiesRegistry;
import ai.vital.vitalsigns.properties.PropertyMetadata;
import ai.vital.vitalsigns.uri.URIGenerator;

public class SQLSingleSegmentQueryHandler extends SQLSelectQueryHandler {

	private final static Logger log = LoggerFactory.getLogger(SQLSingleSegmentQueryHandler.class);
	
	private SQLGraphObjectResolver resolver;

	private static class SingleContainerResults {
		Set<String> uris = null;
		TypeANDResponse typeAndResponse;
	}
	
	protected final static VitalGraphQueryPropertyCriterion EMPTY_CRITERION = new VitalGraphQueryPropertyCriterion();
	
	public SQLSingleSegmentQueryHandler(VitalSqlDataSource dataSource, Connection connection, VitalSelectQuery sq, List<SegmentTable> segmentTables, SQLGraphObjectResolver resolver, QueryStats queryStats) {
		super(dataSource, connection, sq, segmentTables, queryStats);
		this.resolver = resolver;
		
		if(sq instanceof VitalSelectAggregationQuery) {
			
			VitalSelectAggregationQuery vsaq = (VitalSelectAggregationQuery) sq;
			if(vsaq.getAggregationType() == null) throw new NullPointerException("Null aggregation type in " + VitalSelectAggregationQuery.class.getSimpleName());
			aggregationType = vsaq.getAggregationType();
			if(aggregationType != AggregationType.count) {
				if(vsaq.getPropertyURI() == null) throw new NullPointerException("Null vital property in " + VitalSelectAggregationQuery.class.getSimpleName());
			}
			if(vsaq.getPropertyURI() != null) {
				aggregationPropertyURI = vsaq.getPropertyURI();
			}
		} else if(sq.getDistinct()) {
			
			aggregationPropertyURI = sq.getPropertyURI();
		}
		
	}
	
	@Override
	public ResultList execute() throws Exception {
		long start = System.currentTimeMillis();
		ResultList rl = selectQuery();
		if(queryStats != null) {
			queryStats.setQueryTimeMS(System.currentTimeMillis() - start);
		}
		return rl;
		
	}
	
	public ResultList selectQuery() {
		
		if(sq instanceof VitalExportQuery) {

			throw new RuntimeException("Should be implemented by another block");
			
		}
		
		URIResultList outputList = null;
		try {
			outputList = selectQueryInner();
		} catch (Exception e) {
			log.error(e.getLocalizedMessage(), e);
			ResultList rl = new ResultList();
			rl.setStatus(VitalStatus.withError(e.getLocalizedMessage()));
			return rl;
		}
		
		//if it's aggregate function
		ResultList results = null;
        try {
            results = convertURIsListToResultList(outputList);
        } catch (Exception e) {
        	e.printStackTrace();
            ResultList rl = new ResultList();
            rl.setStatus(VitalStatus.withError(e.getLocalizedMessage()));
            return rl;
        }

		
		AggregationType aggType = null;
		Integer agg_count = outputList.agg_count;
		Integer agg_count_distinct = outputList.agg_count_distinct;
		Double agg_sum = outputList.agg_sum;
		
		Double agg_min = outputList.agg_min;
		Double agg_max = outputList.agg_max;
		
		if(sq instanceof VitalSelectAggregationQuery) {
			
			VitalSelectAggregationQuery vsaq = (VitalSelectAggregationQuery)sq;
			aggType = vsaq.getAggregationType();
			
		}
		
		if(aggType != null) {
			
			Double value = null;
			
			if(AggregationType.average == aggType) {
				
				if(agg_count == 0) {
					value = Double.NaN;
				} else {
					value = agg_sum / (double)agg_count;
				}
				
			} else if(AggregationType.count == aggType) {
				if(sq.getDistinct()) {
					value = (double) agg_count_distinct;
				} else {
					value = (double) agg_count;
				}
			} else if(AggregationType.max == aggType) {
				if(agg_max != null) {
					value = agg_max;
				} else {
					value = Double.NaN;
				}
			} else if(AggregationType.min == aggType) {
				if(agg_min != null) {
					value = agg_min;
				} else {
					value = Double.NaN;
				}
			} else if(AggregationType.sum == aggType) {
				value = agg_sum;
			} else {
				throw new RuntimeException("Unhandled aggregation type: " + aggType);
			}
			
			//nasty 
			AggregationResult res = new AggregationResult();
			res.setURI(URIGenerator.generateURI(null, res.getClass(), true));
			res.setProperty("aggregationType", aggType.name());
			res.setProperty("value", value);
			results.getResults().add(new ResultElement(res, 1D));
			
		}
		
		results.setQueryStats(queryStats);
				
		return results;
		
	}
	
	boolean indexOrder = false;
	boolean indexOrderAsc = true;

	boolean uriOrder = false;
	boolean uriOrderAsc = true;
	
	boolean timestampSortProperty = false;
	boolean timestampSortAsc = false;
	
	
	boolean isSingleContainerOptimization = false;
	
	/**
	 * A newer version that 
	 * @param segmentTable
	 * @return
	 * @throws Exception
	 */
	public URIResultList handleSelectQueryAttempt2(SegmentTable segmentTable) throws IOException, SQLException  {
		
		
		List<VitalSortProperty> sortProperties = sq.getSortProperties();
		
		if(agg == null && !distinct) {
			
			
			if( sortProperties.size() == 1) {
				
				VitalSortProperty sp = sortProperties.get(0);
				String pURI = sp.getPropertyURI();
				
				if( VitalSortProperty.INDEXORDER.equals( pURI) ) {
					indexOrder = true;
					indexOrderAsc =  ! sp.isReverse();
				} else if(VitalCoreOntology.URIProp.getURI().equals(pURI) || VitalGraphQueryPropertyCriterion.URI.equals(pURI)) {
					uriOrder = true;
					uriOrderAsc = ! sp.isReverse();
				} else if(VitalCoreOntology.hasTimestamp.getURI().equals(pURI)) {
					timestampSortProperty = true;
					timestampSortAsc = !sp.isReverse();
				}
				
			}
		}
		
		VitalGraphCriteriaContainer criteriaContainer = sq.getCriteriaContainer();
		
		SingleContainerResults results = processSingleContainer(true, segmentTable, criteriaContainer, null);
		
		Set<String> matchingURIs = results.uris; 
		
		URIResultList rl = new URIResultList();
		rl.results = new ArrayList<URIResultElement>();
		rl.offset = sq.getOffset();
		rl.limit = sq.getLimit();
		
		if(distinct) {
			
			//TODO
			List<String> l = new ArrayList<String>(matchingURIs);
			
			List<String> propsToGet = Arrays.asList(sq.getPropertyURI());
			
			PropertiesRegistry pr = VitalSigns.get().getPropertiesRegistry();
			PropertyMetadata pm = pr.getProperty(sq.getPropertyURI());
			if(pm == null) throw new RuntimeException("Property not found in VitalSigns: " + sq.getPropertyURI());
			
			if(sq.getDistinctExpandProperty()) {
				List<PropertyMetadata> subProperties = pr.getSubProperties(pm, false);
				for(PropertyMetadata x : subProperties) {
					propsToGet.add(x.getURI());
				}
			}
			
			Map<String, URIResultElement> m = CoreOperations.getDataAttributes(connection, segmentTable, l, propsToGet, queryStats);
			
			Set<Object> distinctVals = new HashSet<Object>();
			
			for(URIResultElement el : m.values()) {
				
				Map<String, Object> o = el.attributes;
				
				if(o == null) {
					continue;
				}
				
				for(String p : propsToGet) {

					Object v = o.get(p);
					
					if(v != null) {
						distinctVals.add(v);
						break;
					}
					
				}
				
			}
			
			String distinctSort = sq.getDistinctSort();
			
			List<Object> distinctValues = new ArrayList<Object>(distinctVals);
			
			if(distinctSort != null) {
				
				final boolean _asc = "asc".equals(distinctSort);
				
				Collections.sort(distinctValues, new Comparator<Object>() {

					@Override
					public int compare(Object o1, Object o2) {
						Comparable c1 = (Comparable) o1;
						Comparable c2 = (Comparable) o2;
						return _asc ? c1.compareTo(c2) : c2.compareTo(c1);
					}
				});;
				
			}
			
			if(sq.getDistinctFirst()) {
				
				distinctValues = distinctValues.size() > 0 ? distinctValues.subList(0, 1) : distinctValues;
				
			} else if(sq.getDistinctLast()) {
				
				distinctValues = distinctValues.size() > 0 ? distinctValues.subList(distinctValues.size() - 1, distinctValues.size()) : distinctValues;
				
			}
			
			rl.setDistinctValues(distinctValues);
			
		} else if(agg != null) {

			//aggregation collectors
			Integer agg_count = null;
			Integer agg_count_distinct = null;
			Double agg_sum = null;
			
			Double agg_min = null;
			Double agg_max = null;
		
			//do not sort, use indexorder non-inversed
			
			agg_count = new Integer(0);
			agg_sum   = new Double(0);
			
			
			HashSet<Object> uniqueValues = null;
			
			if(sq.isDistinct()) {

				agg_count_distinct = new Integer(0);
				uniqueValues = new HashSet<Object>();
				
				//collect unique values
				
			}
			
			if(aggregationPropertiesURIs.size() == 0) {
				agg_count = matchingURIs.size();
				rl.totalResults = agg_count;
				rl.agg_count = agg_count;
				return rl;
			}
			
			List<String> urisList = new ArrayList<String>(matchingURIs);
			Map<String, URIResultElement> m = CoreOperations.getDataAttributes(connection, segmentTable, urisList, new ArrayList<String>(aggregationPropertiesURIs), queryStats);
			
			for(URIResultElement el : m.values()) {
				
				
				Object aggVal = null;
				
				if(el.attributes != null) {
					
					for(String pURI : aggregationPropertiesURIs) {
						
						aggVal = el.attributes.get(pURI);
						if(aggVal != null) break;
						
					}
					
					
					if(aggVal != null) {
					
						if(uniqueValues != null) {
							uniqueValues.add(aggVal);
						}
						
						agg_count = agg_count + 1;
						
						double val = 0d;
						
						if(aggVal instanceof Number) {
							
							val = ((Number)aggVal).doubleValue();
							
						} else if(aggVal instanceof Date) {
						
							val = ((Date)aggVal).getTime();
							
						} else {
							continue;
						}
							
						agg_sum += val;
							
						if(agg_min == null || agg_min.doubleValue() > val) {
							agg_min = val;
						}
							
						if(agg_max == null || agg_max.doubleValue() < val) {
							agg_max = val;
						}
							
					}
					
					
				}
				
				
			}
			
			
			//perform aggregation
			if(uniqueValues != null) {
				
				agg_count_distinct = uniqueValues.size();
				
				rl.setTotalResults(agg_count_distinct);
				
			} else {
				
				rl.setTotalResults(agg_count);
				
			}
			
			
			rl.setAgg_count(agg_count);
			rl.setAgg_count_distinct(agg_count_distinct);
			rl.setAgg_sum(agg_sum);
			rl.setAgg_min(agg_min);
			rl.setAgg_max(agg_max);
			
		} else {
			
			rl.setTotalResults(matchingURIs.size());
			
			if(sq.isProjectionOnly()) {
				return rl;
			}

			
			int start = sq.getOffset();
			
			int stop = sq.getOffset() + ( sq.getLimit() > 0 ? sq.getLimit() : 10000);
			
			if( sortProperties == null || sortProperties.size() == 0 || indexOrder) {
				
				int i = 0;
				
				
				if(indexOrderAsc) {
					
					for(String u : matchingURIs) {
						
						if(i >= start && i < stop) {
							
							URIResultElement el = new URIResultElement();
							el.URI = u;
							el.segment = segmentTable;
							el.score = 1d;
							rl.getResults().add(el);
							
						}
						
						i++;
						
					}
					
				} else {
					
					List<String> l = new ArrayList<String>(matchingURIs);
					
					for(int j = l.size() -1 ; j >= 0; j-- ) {

						if(i >= start && i < stop) {
							
							URIResultElement el = new URIResultElement();
							el.URI = l.get(j);
							el.segment = segmentTable;
							el.score = 1d;
							rl.getResults().add(el);
							
						}
						
						i++;
						
					}
					
				}
				
			} else if( isSingleContainerOptimization && timestampSortProperty ) {
				
				int i = 0;
				
				for(String u : matchingURIs) {
					
					if(i >= start && i < stop) {
						
						URIResultElement el = new URIResultElement();
						el.URI = u;
						el.segment = segmentTable;
						el.score = 1d;
						rl.getResults().add(el);
						
					}
					
					i++;
					
				}
				
				
			} else if(uriOrder) {

				List<String> l = new ArrayList<String>(matchingURIs);
				
				final boolean _asc = uriOrderAsc;
				
				Collections.sort(l, new Comparator<String>() {
					@Override
					public int compare(String o1, String o2) {
						return _asc ? o1.compareTo(o2) : o2.compareTo(o1); 
					}
				});
				
				int i = 0;
				
				for(String u : l) {
					
					if(i >= start && i < stop) {
						
						URIResultElement el = new URIResultElement();
						el.URI = u;
						el.segment = segmentTable;
						el.score = 1d;
						rl.getResults().add(el);
						
					}
					
					i++;
					
				}
				
			} else {
				
				List<String> propsToGet = new ArrayList<String>();
				
				for(VitalSortProperty sp : sortProperties) {
					if(sp.getPropertyURI().equals(VitalGraphQueryPropertyCriterion.URI) || sp.getPropertyURI().equals(VitalCoreOntology.URIProp.getURI())) {
						
					} else {
						propsToGet.add(sp.getPropertyURI());
					}
				}
				
				//sort output objects via properties
				Map<String, URIResultElement> m = CoreOperations.getDataAttributes(connection, segmentTable, new ArrayList<String>(matchingURIs), propsToGet, queryStats);
				
				List<URIResultElement> l = new ArrayList<URIResultElement>(m.values());
				
				Collections.sort(l, new SortComparator(sortProperties));
				
				for(int i = start; i < Math.min(stop, l.size()); i++) {
					
					rl.getResults().add(l.get(i));
					
				}
				
			}
			
			
		}
		
		return rl;
	}
	
	private SingleContainerResults processSingleContainer(boolean top, SegmentTable segmentTable, VitalGraphCriteriaContainer thisContainer, Set<String> inputURIs) throws IOException, SQLException  {

		SingleContainerResults scr = new SingleContainerResults();
		if( ! ANDConstraintProbing.probeContainer(dataSource, this, segmentTable, thisContainer, queryStats) ) {
			scr.uris = new LinkedHashSet<String>(0);
			return scr;
		}
		
		String tableName = SQLUtils.escapeID(connection, segmentTable.tableName);
		
		//process containers first ?
		Set<String> outputURIs = inputURIs;
		
		List<VitalGraphCriteriaContainer> containers = new ArrayList<VitalGraphCriteriaContainer>();
		List<VitalGraphQueryPropertyCriterion> criteria = new ArrayList<VitalGraphQueryPropertyCriterion>();
		
		for(VitalGraphQueryElement el : new ArrayList<VitalGraphQueryElement>(thisContainer)) {
			
			if(el instanceof VitalGraphQueryPropertyCriterion) {
				
				criteria.add((VitalGraphQueryPropertyCriterion) el);
				
			} else if(el instanceof VitalGraphCriteriaContainer){
		
				VitalGraphCriteriaContainer c = (VitalGraphCriteriaContainer) el;
				
				List<String> orTypeOptimization = ORTypeOptimization.orTypeOptimization(c);
				
				if(orTypeOptimization != null) {

					log.debug("Converting OR container  of type criteria into VitalGraphTypeCriterion");
					
					//don't process it, replace it with VitalGraphTypeCriterion
					VitalGraphQueryTypeCriterion crit = new VitalGraphQueryTypeCriterion();
					crit.setComparator(VitalGraphQueryPropertyCriterion.Comparator.ONE_OF);
					crit.setValue(orTypeOptimization);
					
					criteria.add(crit);
					thisContainer.remove(el);
					thisContainer.add(crit);
					
				} else {

					List<String> notEqualTypeOptimization = NOT_EQUALTypeOptimization.notEqualTypeOptimization(c);
					
					if(notEqualTypeOptimization != null) {
						
						VitalGraphQueryTypeCriterion crit = new VitalGraphQueryTypeCriterion();
						crit.setComparator(VitalGraphQueryPropertyCriterion.Comparator.NONE_OF);
						crit.setValue(notEqualTypeOptimization);
						
						criteria.add(crit);
						thisContainer.remove(el);
						thisContainer.add(crit);
						
					} else {
						
						containers.add(c);
						
					}
					
					

				}
				
				
			}
			
		}
		
		if(criteria.size() == 0 && containers.size() == 0) {
			throw new RuntimeException("Empty query criteria container!");
		}
		
		for(VitalGraphCriteriaContainer container : containers ) {
			
			SingleContainerResults containerResults = processSingleContainer(false, segmentTable, container, outputURIs);
			
			Set<String> subResults = containerResults.uris;
			
			if(thisContainer.getType() == QueryContainerType.and ) {
				//exit immediately
				if(subResults.size() < 1) {
					return containerResults;
				}
				
				if(outputURIs == null) {
					outputURIs = subResults;
				} else {
					outputURIs.retainAll(subResults);
				}
				
				if(outputURIs.size() < 1) {
					SingleContainerResults r = new SingleContainerResults();
					r.uris = outputURIs;
					return r; 
				}
				
			} else {
				
				//or it toget
				if(outputURIs == null) {
					outputURIs = subResults;
				} else {
					outputURIs.addAll(subResults);
				}
				
			}
			
		}
		
		TypeANDResponse typePropTypes = VitalTypeANDConstraintOptimization.isVitalTypePropertyANDContainer(thisContainer);
		
		if(typePropTypes != null) {
			log.debug("Applying vitaltype+property in AND, types: {}", typePropTypes);
		}
		
		
		List<VitalGraphQueryPropertyCriterion> otherCriteria = new ArrayList<VitalGraphQueryPropertyCriterion>();
		
		for(VitalGraphQueryPropertyCriterion criterion : criteria) {
			
			
			if(typePropTypes != null) {
				if(criterion instanceof VitalGraphQueryTypeCriterion) {
					continue;
				}
				if(typePropTypes.coveredCriteria.contains(criterion)) {
					continue;
				}
			}
			
			otherCriteria.add(criterion);
		}
		
		if(typePropTypes != null && otherCriteria.size() == 0) {
			//artificial null criterion to process other single criteria
			otherCriteria.add(EMPTY_CRITERION);
		}
		
		for(VitalGraphQueryPropertyCriterion criterion : otherCriteria) {
			
			CoreSelectQuery c = new CoreSelectQuery();
			c.tableName = tableName;
			processCriterion(c, null, (VitalGraphQueryPropertyCriterion) criterion, "", true, true, typePropTypes, false);
			
			ResultSet rs = null;
			
			PreparedStatement stmt = null;
			
			Set<String> subResults = new LinkedHashSet<String>();
			
			try {
				
				
				String mainQuery = c.queryTemplate.toString();
				
				if(top && typePropTypes != null && otherCriteria.size() == 1) {
					
					isSingleContainerOptimization = true;
					
					if(timestampSortProperty) {
						mainQuery += " ORDER BY " + COLUMN_TSTAMP + " " + (timestampSortAsc ? " ASC " : " DESC ");
					}
					
					//TODO offset / limit optimization as next step optimization ?
//					mainQuery += (sq.getLimit() > 0 ? ( " LIMIT " + sq.getLimit() ) : "" ) + " OFFSET " + sq.getOffset();
					
				}
				
				stmt = connection.prepareStatement(mainQuery);
				
				int i = 1;
				
				for(Object v : c.substitutes) {
					stmt.setObject(i, v);
					i++;
				}
				
				
				if(log.isDebugEnabled()) log.debug("Single criterion query, criterion {}, sql: {}", criterion.toString(), stmt);
				
				long start = System.currentTimeMillis();
				
				rs = stmt.executeQuery();
				
				if(queryStats != null) {
					long time = queryStats.addDatabaseTimeFrom(start);
					if(queryStats.getQueriesTimes() != null) queryStats.getQueriesTimes().add(new QueryTime(criterion.toString(), stmt.toString(), time));
				}
				
				if(log.isDebugEnabled()) log.debug("Single criterion query time: {}ms", System.currentTimeMillis() - start);
				
				while(rs.next()) {
					
					
					String uri = rs.getString(COLUMN_URI);
				
					subResults.add(uri);
					
				}
				
				//check if it's a negated query
				VitalGraphQueryPropertyCriterion.Comparator comparator = criterion.getComparator();
				
				if( ( !criterion.isNegative() && ( comparator == VitalGraphQueryPropertyCriterion.Comparator.NOT_CONTAINS || comparator == VitalGraphQueryPropertyCriterion.Comparator.NOT_EXISTS) )  
						|| (criterion.isNegative() && (comparator == VitalGraphQueryPropertyCriterion.Comparator.CONTAINS || comparator == VitalGraphQueryPropertyCriterion.Comparator.EXISTS))) {
				
					if(thisContainer.getType() == QueryContainerType.and) {
						
						
						if(outputURIs == null) {
							
							//need to select all uris!
							outputURIs = CoreOperations.getAllURIs(connection, segmentTable, queryStats);
							outputURIs.removeAll(subResults);
							
						} else {
							
							outputURIs.removeAll(subResults);
							
						}
						
						if(outputURIs.size() < 1) {
							SingleContainerResults r = new SingleContainerResults();
							r.uris = outputURIs;
							r.typeAndResponse = typePropTypes;
							return r;
						}
						
					} else {

						if(outputURIs == null) {
						
							outputURIs = CoreOperations.getAllURIs(connection, segmentTable, queryStats);
							outputURIs.removeAll(subResults);

						} else {

							Set<String> p = CoreOperations.getAllURIs(connection, segmentTable, queryStats);
							p.removeAll(subResults);
							
							outputURIs.addAll(p);
							
						}
						
					}
					
				} else {
					
					if(thisContainer.getType() == QueryContainerType.and ) {
						//exit immediately
						if(subResults.size() < 1) {
							SingleContainerResults r = new SingleContainerResults();
							r.uris = subResults;
							r.typeAndResponse = typePropTypes;
							return r;
						}
						
						if(outputURIs == null) {
							outputURIs = subResults;
						} else {
							outputURIs.retainAll(subResults);
						}
						
						if(outputURIs.size() < 1) {
							SingleContainerResults r = new SingleContainerResults();
							r.uris = outputURIs;
							r.typeAndResponse = typePropTypes;
							return r;
						}
						
					} else {
						
						//or it toget
						if(outputURIs == null) {
							outputURIs = subResults;
						} else {
							outputURIs.addAll(subResults);
						}
						
					}
					
				}
				


			} catch(SQLSyntaxErrorException ex) {
				
				log.error("SQL syntax error: " + c.queryTemplate.toString());
				
				throw ex;
				
			} finally {
				SQLUtils.closeQuietly(stmt, rs);
			}
			
			
		}
		
		SingleContainerResults r = new SingleContainerResults();
		r.uris = outputURIs;
		r.typeAndResponse = typePropTypes;
		return r;
		
	}

	/**
	 * Some improvement to multi table union query but still slow
	 * @param segmentTable
	 * @return
	 * @throws IOException
	 * @throws SQLException
	 */
	@Deprecated
	public URIResultList handleSelectQueryAttempt1(SegmentTable segmentTable) throws IOException, SQLException {
		
		URIResultList rl = new URIResultList();
		rl.setOffset(this.sq.getOffset());
		rl.setLimit(this.sq.getLimit());
		
		
		//always prepare count query ?
//		CoreSelectQuery coreCountQuery = prepareCoreQuery(sq.getCriteriaContainer(), segmentTable, "", true);
		
		CoreSelectQuery coreCountQuery = buildSingleSegmentQuery(sq.getCriteriaContainer(), segmentTable, true);
		
		PreparedStatement stmt = null;
		
		ResultSet rs = null;
		
		try {
			
			int i = 1;
			
			
			if(true || sq.isProjectionOnly() || (aggregationType == AggregationType.count && aggregationPropertyURI == null)) {
				
				stmt = connection.prepareStatement(coreCountQuery.queryTemplate.toString());
				
				for(Object v : coreCountQuery.substitutes) {
					stmt.setObject(i, v);
					i++;
				}
				
				if(log.isDebugEnabled()) log.debug("Select count query: {}", stmt);
				
				long start = System.currentTimeMillis();
				
				rs = stmt.executeQuery();
				
				if(queryStats != null) {
					long time = queryStats.addDatabaseTimeFrom(start);
					if(queryStats.getQueriesTimes() != null) queryStats.getQueriesTimes().add(new QueryTime("COUNT: " + sq.debugString(), stmt.toString(), time));
				}
				
				rs.next();
				rl.setTotalResults(rs.getInt(1));
				rs.close();
				stmt.close();
				
				if(sq.isProjectionOnly()) {
					return rl;
				}
				
				
				if(agg != null && aggregationType == AggregationType.count /*&& aggregationPropertyURI == null*/) {
					
					//nasty 
					rl.agg_count = rl.totalResults.intValue();
//					AggregationResult res = new AggregationResult();
//					res.setURI(URIGenerator.generateURI(null, res.getClass(), true));
//					res.setProperty("aggregationType", aggregationType.name());
//					res.setProperty("value", rl.getTotalResults().doubleValue());
//					rl.getResults().add(new ResultElement(res, 1D));
//					
					return rl;
					
				}
				
			}
			
			
			CoreSelectQuery csqx = buildSingleSegmentQuery(sq.getCriteriaContainer(), segmentTable, false);
//			CoreSelectQuery csqx = prepareCoreQuery( sq.getCriteriaContainer(), segmentTable, "    ", false);
			
			String mainQuery = csqx.queryTemplate.toString();
			
			stmt = connection.prepareStatement(mainQuery);
			
			i = 1;
			
			for(Object v : csqx.substitutes) {
				stmt.setObject(i, v);
				i++;
			}
			
			
			if(log.isDebugEnabled()) log.debug("Select objects query: {}", stmt);
			
			long start = System.currentTimeMillis();
			
			rs = stmt.executeQuery();
			
			if(queryStats != null) {
				long time = queryStats.addDatabaseTimeFrom(start);
				if(queryStats.getQueriesTimes() != null) queryStats.getQueriesTimes().add(new QueryTime("SELECT: " + sq.debugString(), stmt.toString(), time));
			}
			
			if(agg != null) {
				
				rs.next();
				
				Number aggValue = (Number) rs.getObject(1);
				
				if(aggregationType == AggregationType.average) {
//					rl.
				} else if(aggregationType == AggregationType.count) {
					rl.agg_count = aggValue.intValue();
				} else if(aggregationType == AggregationType.max) {
					rl.agg_max = aggValue.doubleValue();
				} else if(aggregationType == AggregationType.min) {
					rl.agg_min = aggValue.doubleValue();
				} else if(aggregationType == AggregationType.sum) {
					rl.agg_sum = aggValue.doubleValue();
				}
				
				//nasty 
//				AggregationResult res = new AggregationResult();
//				res.setURI(URIGenerator.generateURI(null, res.getClass(), true));
//				res.setProperty("aggregationType", aggregationType.name());
//				res.setProperty("value", aggValue.doubleValue());
//				rl.getResults().add(new ResultElement(res, 1D));
				
			} else if(distinct) {
				
//				double score = 0d;
				
				List<Object> distinctValues = new ArrayList<Object>();
				
				while(rs.next()) {
					
					
					Object dv = rs.getObject(column_distinct);
				
					distinctValues.add(dv);
					
//					GraphMatch gm = (GraphMatch) new GraphMatch().generateURI((VitalApp)null);
//					gm.setProperty("value", dv);
//					
//					rl.getResults().add(new ResultElement(gm, score));
//					score += 1d;
					
					
					
				}
				
				rl.setDistinctValues(distinctValues);
				
				return rl;
				
			} else {
				
				
				int j = 0;
				
				List<URIResultElement> results = new ArrayList<URIResultElement>();
				
				while(rs.next()) {
					
					String uri = rs.getString(COLUMN_URI);
				
					URIResultElement el = new URIResultElement();
					el.segment = segmentTable;
					el.URI = uri;
					
					results.add(el);
					
				}

				rl.setResults(results);
				
//				List<GraphObject> fromSql = null;
//				for( Entry<String, List<String>> entry : utp.map.entrySet() ) {
//					
//					SegmentTable st = new SegmentTable(null, entry.getKey());
//					List<GraphObject> graphObjectsBatch = CoreOperations.getGraphObjectsBatch(connection, st, entry.getValue());
//					if(fromSql == null) {
//						fromSql = graphObjectsBatch;
//					} else {
//						fromSql.addAll(graphObjectsBatch);
//					}
//					
//				}
//				
//				if(fromSql == null) fromSql = Collections.emptyList();
//				
//				if(coreFullQueries.size() > 1) {
//					
//					Collections.sort(fromSql, new UriTableComparator(utp.order));
//					
//				}
//				
//				
//				for(GraphObject g: fromSql) {
//					rl.getResults().add(new ResultElement(g, 1D));
//				}
				
			}
			
		} finally {
			SQLUtils.closeQuietly(stmt, rs);
		}
		
		
		return rl;
		
	}
	
	static Comparator<URIResultElement> uriResultElementComparator = new Comparator<URIResultElement>(){
		
		public int compare(URIResultElement e1, URIResultElement e2) {
			
			int c = new Double(e1.score).compareTo(e2.score);
			
			if(c != 0) return c;
			
			return e1.URI.compareTo(e2.URI);
			
		}
		
	};

	
	public URIResultList selectQueryInner() throws IOException, SQLException {

		URIResultList outputList = null;
		
		AggregationType aggType = null;
		Integer agg_count = null;
		Double agg_sum = null;
		
		Double agg_min = null;
		Double agg_max = null;
		
		List distinctValues = null;
		
		if(sq instanceof VitalSelectAggregationQuery) {
			
			if(sq.isProjectionOnly()) {
				throw new RuntimeException("Cannot use projection only in VitalSelectAggregationQuery");
			}
			
			//if it's aggregation the ignore limit and skip
			VitalSelectAggregationQuery vsaq = (VitalSelectAggregationQuery)sq;
			aggType = vsaq.getAggregationType();
			agg_count = 0;
			agg_sum = 0d;
			
		}
		
		
		//if only one lucene segment specified just assemble a single page 
		if(segments.size() == 1) {
			
			outputList = handleSelectQueryAttempt2(segments.get(0));
			
			distinctValues = outputList.getDistinctValues();
			
			agg_count = outputList.agg_count;
			agg_sum = outputList.agg_sum;
			
			agg_min = outputList.agg_min;
			agg_max = outputList.agg_max;
			
//			if(query.isProjectionOnly()) {
//				ResultList rs = new ResultList()
//				rs.totalResults = outputList.totalResults
//				return ou
//			} 
			
		} else {

			//for projections just sum up the elements
			if(sq.isProjectionOnly()) {
				
				outputList = new URIResultList();
				outputList.totalResults = 0	;
				outputList.results = new ArrayList<URIResultElement>(0);
				
				for( SegmentTable segment : segments ) {
				
					URIResultList  sublist = handleSelectQueryAttempt2(segment);
					outputList.totalResults	 += sublist.totalResults;
					outputList.offset = sq.getOffset();
					outputList.limit = sq.getLimit();
				}
				
			} else {
				
				int initialOffset = sq.getOffset();
				int initialLimit = sq.getLimit();
			
				outputList = new URIResultList();
				outputList.limit = initialLimit;
				outputList.offset = initialOffset;
				int _size = initialLimit-initialOffset; 
				outputList.results = new ArrayList<URIResultElement>(_size < 10000 && _size > 0 ? _size : 10000);
			
				//query with offset = 0
				sq.setOffset(0);
				
				int totalResults = 0;
				
				int maxLength = initialOffset + initialLimit;
				
				if(maxLength < 1) maxLength = 1; 
				
				PriorityBuffer priorityBuffer = new PriorityBuffer(maxLength, true,  uriResultElementComparator);
				
				for( SegmentTable segment : segments ) {

					URIResultList sublist = handleSelectQueryAttempt2(segment);					
					
					List subDistinctValues = sublist.getDistinctValues();
					
					if(subDistinctValues != null) {
						if(distinctValues != null) {
							for(Object dv : subDistinctValues) {
								if(!distinctValues.contains(dv)) {
									distinctValues.add(dv);
								}
							}
						} else {
							distinctValues = subDistinctValues;
						}
					}
					
					totalResults += sublist.totalResults;
					
					if(aggType != null) {
						
						agg_count += sublist.agg_count;
						agg_sum   += sublist.agg_sum;
						
						if(sublist.agg_min != null && (agg_min == null || agg_min.doubleValue() > sublist.agg_min.doubleValue())) {
							agg_min = sublist.agg_min;
						}
						
						if(sublist.agg_max != null && (agg_max == null || agg_max.doubleValue() < sublist.agg_max.doubleValue() )) {
							agg_max = sublist.agg_max;
						}
						
					} else {
					
						for(URIResultElement el : sublist.results) {
							
							if(priorityBuffer.size() == maxLength) {
								priorityBuffer.remove();
							}
							
							priorityBuffer.add(el);
							
						}
						
					}
					
					
				}
				
				if(distinctValues != null && sq.getDistinctSort() != null) {
					
					boolean ascNotDesc = VitalSelectQuery.asc.equals(sq.getDistinctSort());
					
					//sort them
					Collections.sort(distinctValues, new ai.vital.lucene.query.LuceneSelectQueryHandler.DistinctValuesComparator(ascNotDesc));
					
				}
				
				if(distinctValues != null) {
					if(sq.getDistinctFirst() && distinctValues.size() > 1) {
						distinctValues = new ArrayList<Object>(Arrays.asList(distinctValues.get(0)));
					} else if(sq.getDistinctLast() && distinctValues.size() > 1){
						distinctValues = new ArrayList<Object>(Arrays.asList(distinctValues.get(distinctValues.size()-1)));
					}
				}
				
				outputList.distinctValues = distinctValues;
				
				outputList.totalResults = totalResults;
				
				outputList.agg_count = agg_count;
				outputList.agg_sum = agg_sum;
				outputList.agg_max = agg_max;
				outputList.agg_min = agg_min;
				
				//copy the results into list reverse order
				int index = 0;
				
				while(priorityBuffer.size() > 0) {
				
					URIResultElement el = (URIResultElement) priorityBuffer.remove();
					
					if(index++ >= initialOffset) {
						
						outputList.results.add(0, el);
						
					}	
					
				}

			}

		}
		
		return outputList;
		
	}
	
	
	ResultList convertURIsListToResultList(URIResultList outputList) throws Exception {
		
		if(outputList.results == null ) outputList.results = new ArrayList<URIResultElement>();
		
		ResultList results = new ResultList();
		results.setLimit(outputList.limit);
		results.setOffset(outputList.offset);
		results.setTotalResults(outputList.totalResults);
		
		if(outputList.distinctValues != null) {
			
			double score = 0d;
			for(Object dv : outputList.distinctValues) {
				
				GraphMatch gm = (GraphMatch) new GraphMatch().generateURI((VitalApp)null);
				gm.setProperty("value", dv);
				
				results.getResults().add(new ResultElement(gm, score));
				score += 1d;
				
			}
			
			
			return results;
			
		}
		
		//convert uris list into result list
		Set<String> urisToFetch = new HashSet<String>(outputList.results.size());
		
		Map<SegmentTable, Set<String>> segmentToURIs = new HashMap<SegmentTable, Set<String>>();
		Map<String, GraphObject> resolvedGraphObjects = new HashMap<String, GraphObject>();
		
		for(URIResultElement e : outputList.results) {
			
			Set<String> uris = segmentToURIs.get(e.segment);
			if(uris == null) {
				uris = new HashSet<String>();
				segmentToURIs.put(e.segment, uris);
			}

			uris.add(e.URI);
						
		}
		
		
		for(Entry<SegmentTable, Set<String>> e : segmentToURIs.entrySet()) {
	
			List<GraphObject> gos = CoreOperations.getGraphObjectsBatch(connection, e.getKey(), new ArrayList<String>(e.getValue()), resolver, queryStats);
			
			for(GraphObject g : gos) {
				resolvedGraphObjects.put(g.getURI(), g);
			}
			
//			//special case for cache segment 
//			if(e.getKey().getID().equals(VitalSigns.CACHE_DOMAIN)) {
//				
//				for(String uri : e.getValue()) {
//					
//					GraphObject g = GlobalHashTable.get().get(uri);
//					
//					if(g != null) {
//						resolvedGraphObjects.put(g.getURI(), g);
//					}
//					
//				}
//				
//			} else {
//			
//				if( e.getKey().getConfig().isStoreObjects() ) {
//	
//				
//				
//					List<GraphObject> gos = e.getKey().getGraphObjectsBatch(e.getValue());
//							
//					for(GraphObject g : gos) {
//						resolvedGraphObjects.put(g.getURI(), g);
//					}
//					
//				} else {
//					
//					//fake resolving, just listing URIs
//					
//					for(String uri : e.getValue()) {
//						GraphObject g = new VITAL_Node();
//						g.setURI(uri);
//						resolvedGraphObjects.put(uri, g);
//					}
//					
//				}
//				
//			}
			
		}
		
		results.setResults(new ArrayList<ResultElement>(outputList.results.size()));
		
		for(URIResultElement el : outputList.results) {
			
			GraphObject g = resolvedGraphObjects.get(el.URI);
			if(g == null) continue;
			
			results.getResults().add(new ResultElement(g, el.score));
			
		}
		
		return results;
		
	}
	
}
