package ai.vital.sql.query;

import static ai.vital.sql.model.VitalSignsToSqlBridge.COLUMN_CHANNEL_URI;
import static ai.vital.sql.model.VitalSignsToSqlBridge.COLUMN_NAME;
import static ai.vital.sql.model.VitalSignsToSqlBridge.COLUMN_TSTAMP;
import static ai.vital.sql.model.VitalSignsToSqlBridge.COLUMN_URI;
import static ai.vital.sql.model.VitalSignsToSqlBridge.COLUMN_VALUE_BOOLEAN;
import static ai.vital.sql.model.VitalSignsToSqlBridge.COLUMN_VALUE_BOOLEAN_MULTIVALUE;
import static ai.vital.sql.model.VitalSignsToSqlBridge.COLUMN_VALUE_DATE;
import static ai.vital.sql.model.VitalSignsToSqlBridge.COLUMN_VALUE_DATE_MULTIVALUE;
import static ai.vital.sql.model.VitalSignsToSqlBridge.COLUMN_VALUE_DOUBLE;
import static ai.vital.sql.model.VitalSignsToSqlBridge.COLUMN_VALUE_DOUBLE_MULTIVALUE;
import static ai.vital.sql.model.VitalSignsToSqlBridge.COLUMN_VALUE_FLOAT;
import static ai.vital.sql.model.VitalSignsToSqlBridge.COLUMN_VALUE_FLOAT_MULTIVALUE;
import static ai.vital.sql.model.VitalSignsToSqlBridge.COLUMN_VALUE_GEOLOCATION;
import static ai.vital.sql.model.VitalSignsToSqlBridge.COLUMN_VALUE_GEOLOCATION_MULTIVALUE;
import static ai.vital.sql.model.VitalSignsToSqlBridge.COLUMN_VALUE_INTEGER;
import static ai.vital.sql.model.VitalSignsToSqlBridge.COLUMN_VALUE_INTEGER_MULTIVALUE;
import static ai.vital.sql.model.VitalSignsToSqlBridge.COLUMN_VALUE_LONG;
import static ai.vital.sql.model.VitalSignsToSqlBridge.COLUMN_VALUE_LONG_MULTIVALUE;
import static ai.vital.sql.model.VitalSignsToSqlBridge.COLUMN_VALUE_OTHER;
import static ai.vital.sql.model.VitalSignsToSqlBridge.COLUMN_VALUE_OTHER_MULTIVALUE;
import static ai.vital.sql.model.VitalSignsToSqlBridge.COLUMN_VALUE_STRING;
import static ai.vital.sql.model.VitalSignsToSqlBridge.COLUMN_VALUE_STRING_MULTIVALUE;
import static ai.vital.sql.model.VitalSignsToSqlBridge.COLUMN_VALUE_TRUTH;
import static ai.vital.sql.model.VitalSignsToSqlBridge.COLUMN_VALUE_TRUTH_MULTIVALUE;
import static ai.vital.sql.model.VitalSignsToSqlBridge.COLUMN_VALUE_URI;
import static ai.vital.sql.model.VitalSignsToSqlBridge.COLUMN_VALUE_URI_MULTIVALUE;
import static ai.vital.sql.model.VitalSignsToSqlBridge.COLUMN_VITALTYPE;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ai.vital.sql.connector.VitalSqlDataSource;
import ai.vital.sql.dao.CoreOperations;
import ai.vital.sql.model.SegmentTable;
import ai.vital.sql.model.VitalSignsToSqlBridge;
import ai.vital.sql.query.VitalTypeANDConstraintOptimization.TypeANDResponse;
import ai.vital.sql.utils.SQLUtils;
import ai.vital.vitalservice.query.AggregationType;
import ai.vital.vitalservice.query.QueryContainerType;
import ai.vital.vitalservice.query.QueryStats;
import ai.vital.vitalservice.query.ResultElement;
import ai.vital.vitalservice.query.ResultList;
import ai.vital.vitalservice.query.VitalGraphCriteriaContainer;
import ai.vital.vitalservice.query.VitalGraphQueryElement;
import ai.vital.vitalservice.query.VitalGraphQueryPropertyCriterion;
import ai.vital.vitalservice.query.VitalGraphQueryPropertyCriterion.Comparator;
import ai.vital.vitalservice.query.VitalGraphQueryTypeCriterion;
import ai.vital.vitalservice.query.VitalSelectAggregationQuery;
import ai.vital.vitalservice.query.VitalSelectQuery;
import ai.vital.vitalservice.query.VitalSortProperty;
import ai.vital.vitalsigns.VitalSigns;
import ai.vital.vitalsigns.classes.ClassMetadata;
import ai.vital.vitalsigns.datatype.Truth;
import ai.vital.vitalsigns.model.AggregationResult;
import ai.vital.vitalsigns.model.GraphMatch;
import ai.vital.vitalsigns.model.GraphObject;
import ai.vital.vitalsigns.model.VitalApp;
import ai.vital.vitalsigns.model.properties.Property_URIProp;
import ai.vital.vitalsigns.model.property.BooleanProperty;
import ai.vital.vitalsigns.model.property.DateProperty;
import ai.vital.vitalsigns.model.property.DoubleProperty;
import ai.vital.vitalsigns.model.property.FloatProperty;
import ai.vital.vitalsigns.model.property.GeoLocationProperty;
import ai.vital.vitalsigns.model.property.IProperty;
import ai.vital.vitalsigns.model.property.IntegerProperty;
import ai.vital.vitalsigns.model.property.LongProperty;
import ai.vital.vitalsigns.model.property.OtherProperty;
import ai.vital.vitalsigns.model.property.StringProperty;
import ai.vital.vitalsigns.model.property.TruthProperty;
import ai.vital.vitalsigns.model.property.URIProperty;
import ai.vital.vitalsigns.ontology.VitalCoreOntology;
import ai.vital.vitalsigns.properties.PropertyMetadata;
import ai.vital.vitalsigns.uri.URIGenerator;

/**
 * This implementetaion maps vital SELECT query into exactly one SQL query statement. 
 * Query over multiple segments is covered by sql UNION
 *
 */
public class SQLSelectQueryHandler {

	private final static Logger log = LoggerFactory.getLogger(SQLSelectQueryHandler.class);
	
	//for negation removal
	static Map<Comparator, Comparator> negatedComparatorsMap = new HashMap<Comparator, Comparator>();
	static {
		negatedComparatorsMap.put(Comparator.CONTAINS, Comparator.NOT_CONTAINS);
		negatedComparatorsMap.put(Comparator.EQ, Comparator.NE);
		negatedComparatorsMap.put(Comparator.EXISTS, Comparator.NOT_EXISTS);
		negatedComparatorsMap.put(Comparator.GE, Comparator.LT);
		negatedComparatorsMap.put(Comparator.GT, Comparator.LE);
		negatedComparatorsMap.put(Comparator.LE, Comparator.GT);
		negatedComparatorsMap.put(Comparator.LT, Comparator.GE);
		negatedComparatorsMap.put(Comparator.NE, Comparator.EQ);
		negatedComparatorsMap.put(Comparator.NE, Comparator.EQ);
		negatedComparatorsMap.put(Comparator.NOT_CONTAINS, Comparator.CONTAINS);
		negatedComparatorsMap.put(Comparator.NOT_EXISTS, Comparator.EXISTS);
	}
	
	
	public final 
	
	static Set<Comparator> unsupportedComparatorsSet = Collections.emptySet();

	private static final String column_sourceTable = "source_table";
	
	private static final String column_aggregation = "aggregation_value";
	
	protected static final String column_distinct = "distinct_value";
	
	static Set<Comparator> nonNegableComparatorsSet = new HashSet<Comparator>(Arrays.asList(
		Comparator.CONTAINS_CASE_INSENSITIVE,
		Comparator.CONTAINS_CASE_SENSITIVE,
		Comparator.EQ_CASE_INSENSITIVE
	));
	
	VitalSelectAggregationQuery agg = null;
	
	VitalSelectQuery sq = null;

	boolean distinct = false;
	
	String aggregationPropertyURI = null;
	
	AggregationType aggregationType = null;
	
	Set<String> aggregationPropertiesURIs = new HashSet<String>();
	
	protected List<SegmentTable> segments;

	protected Connection connection;

	private Map<String, Set<String>> type2ExpandedTypes = new HashMap<String, Set<String>>();

	private Map<String, Set<String>> property2ExpandedProperties = new HashMap<String, Set<String>>();
	
	//calculated on first pass
	private List<SortData> sortData = null;

	protected VitalSqlDataSource dataSource;

	protected QueryStats queryStats;
	
	private static class SortData {
		
		public SortData(VitalSortProperty vsp, String label) {
			super();
			this.vsp = vsp;
			this.label = label;
		}

		VitalSortProperty vsp;
		
		String label;
		
		
	}
	
	public SQLSelectQueryHandler(VitalSqlDataSource dataSource, Connection connection, VitalSelectQuery sq, List<SegmentTable> segments, QueryStats queryStats) {

		this.dataSource = dataSource;
		
		this.connection = connection;
		
		this.segments = segments;
		
		this.queryStats = queryStats;
		
		if(sq instanceof VitalSelectAggregationQuery) {
			
//			if(!segment.getConfig().isStoreObjects()) throw new RuntimeException("Aggregation functions are only supported in an index which also stores field values.");
			
			VitalSelectAggregationQuery vsaq = (VitalSelectAggregationQuery) sq;
			if(vsaq.getAggregationType() == null) throw new NullPointerException("Null aggregation type in " + VitalSelectAggregationQuery.class.getSimpleName());
			aggregationType = vsaq.getAggregationType();
			if(aggregationType != AggregationType.count) {
				if(vsaq.getPropertyURI() == null) throw new NullPointerException("Null vital property in " + VitalSelectAggregationQuery.class.getSimpleName());
				aggregationPropertiesURIs.add(vsaq.getPropertyURI());
			}
			if(vsaq.getPropertyURI() != null) {
				aggregationPropertyURI = vsaq.getPropertyURI();
				aggregationPropertiesURIs.add(aggregationPropertyURI);
			}
			
		} else if(sq.getDistinct()) {
			
//			if(!segment.getConfig().isStoreObjects()) throw new RuntimeException("Distinct values selector is only supported in an index which also stored field values");
			
			distinct = true;
			
			aggregationPropertyURI = sq.getPropertyURI();
			aggregationPropertiesURIs.add(aggregationPropertyURI);
			
			
			if(sq.isDistinctExpandProperty()) {
				
				throw new RuntimeException("distinct with expanded properties unsupported");
				
//				
//				
//				PropertyMetadata pm = VitalSigns.get().getPropertiesRegistry().getProperty(aggregationPropertyURI);
//				if(pm == null) throw new RuntimeException("Property not found: " + aggregationPropertyURI);
//				for(PropertyMetadata p : VitalSigns.get().getPropertiesRegistry().getSubProperties(pm, false)) {
//					aggregationPropertiesURIs.add(p.getPattern().getURI());
//				}
				
			}
			
		}
		
		
		
		

		try {
			sq = (VitalSelectQuery) sq.clone();
		} catch (CloneNotSupportedException e) {
			throw new RuntimeException();
		}
		
		//analyze query
		queryPreprocessor1(sq.getCriteriaContainer());
		
		//expands subproperties
		queryPreprocessor2(sq.getCriteriaContainer());
		
		queryProcessor3(null, sq.getCriteriaContainer());
		
		queryProcessor4(null, sq.getCriteriaContainer());
		
		//analyze the query
		
		if(sq instanceof VitalSelectAggregationQuery) {
			this.agg = (VitalSelectAggregationQuery) sq;
		}
		this.sq = sq;
		
		
	}
	
	public ResultList execute() throws Exception {
		return executeAsASingleQuery();
		
	}
	
	private ResultList executeAsASingleQuery() throws SQLException {
		
//		List<CoreSelectQuery>
		List<CoreSelectQuery> coreCountQueries = new ArrayList<CoreSelectQuery>();
		
		for(SegmentTable st : segments) {
			CoreSelectQuery csq = prepareCoreQuery( sq.getCriteriaContainer(), st, "    ", true);
			coreCountQueries.add(csq);
		}
		
		
		
		PreparedStatement stmt = null;
		
		ResultSet rs = null;
		
		ResultList rl = new ResultList();
		rl.setLimit(sq.getLimit());
		rl.setOffset(sq.getOffset());
		
		try {
			
			String countQuery = buildQuery(coreCountQueries, true);
			stmt = connection.prepareStatement(countQuery);

			
			int i = 1;
			
			if(true || sq.isProjectionOnly()) {
				
				for(CoreSelectQuery csq : coreCountQueries) {
					for(Object v : csq.substitutes) {
						stmt.setObject(i, v);
						i++;
					}
				}
				
				if(log.isDebugEnabled()) {
					log.debug("Select count query: {}", stmt);
				}
				
				rs = stmt.executeQuery();
				rs.next();
				rl.setTotalResults(rs.getInt(1));
				rs.close();
				stmt.close();
				
				if(sq.isProjectionOnly()) {
					return rl;
				}
				
				
				if(agg != null && aggregationType == AggregationType.count && aggregationPropertyURI == null) {
					
					//nasty 
					AggregationResult res = new AggregationResult();
					res.setURI(URIGenerator.generateURI(null, res.getClass(), true));
					res.setProperty("aggregationType", aggregationType.name());
					res.setProperty("value", rl.getTotalResults().doubleValue());
					rl.getResults().add(new ResultElement(res, 1D));
					
					return rl;
				}
				
			}
			
			
			
			List<CoreSelectQuery> coreFullQueries = new ArrayList<CoreSelectQuery>();
			
			for(SegmentTable st : segments) {
				CoreSelectQuery csq = prepareCoreQuery( sq.getCriteriaContainer(), st, "    ", false);
				coreFullQueries.add(csq);
			}
			
			String mainQuery = buildQuery(coreFullQueries, false);
			stmt = connection.prepareStatement(mainQuery);
			i = 1;
			for(CoreSelectQuery csq : coreFullQueries) {
				for(Object v : csq.substitutes) {
					stmt.setObject(i, v);
					i++;
				}
			}
			
			if(log.isDebugEnabled()) {
				log.debug("Select objects query: {}", stmt);
			}
			
			rs = stmt.executeQuery();
			
			if(agg != null) {
				
				rs.next();
				Number aggValue = (Number) rs.getObject(1);
				
				//nasty 
				AggregationResult res = new AggregationResult();
				res.setURI(URIGenerator.generateURI(null, res.getClass(), true));
				res.setProperty("aggregationType", aggregationType.name());
				res.setProperty("value", aggValue.doubleValue());
				rl.getResults().add(new ResultElement(res, 1D));
				
			} else if(distinct) {
				
				double score = 0d;
				
				while(rs.next()) {
					
					Object dv = rs.getObject(column_distinct);
					
					GraphMatch gm = (GraphMatch) new GraphMatch().generateURI((VitalApp)null);
					gm.setProperty("value", dv);
					
					rl.getResults().add(new ResultElement(gm, score));
					score += 1d;
					
					
				}
				
				return rl;
				
			} else {
				
				UriTablePairs utp = unpackURIResults(rs);
				
				List<GraphObject> fromSql = null;
				for( Entry<String, List<String>> entry : utp.map.entrySet() ) {
					
					SegmentTable st = new SegmentTable(null, entry.getKey());
					List<GraphObject> graphObjectsBatch = CoreOperations.getGraphObjectsBatch(connection, st, entry.getValue(), null, queryStats);
					if(fromSql == null) {
						fromSql = graphObjectsBatch;
					} else {
						fromSql.addAll(graphObjectsBatch);
					}
					
				}
				
				if(fromSql == null) fromSql = Collections.emptyList();
				
				if(coreFullQueries.size() > 1) {
					
					Collections.sort(fromSql, new UriTableComparator(utp.order));
					
				}
				
				
				for(GraphObject g: fromSql) {
					rl.getResults().add(new ResultElement(g, 1D));
				}
				
			}
			
			
			
		} finally {
			SQLUtils.closeQuietly(stmt, rs);
		}
		
		return rl;
		
	}
	
	private static class UriTableComparator implements java.util.Comparator<GraphObject> {

		Map<String, Integer> order;
		
		
		public UriTableComparator(Map<String, Integer> order) {
			super();
			this.order = order;
		}



		@Override
		public int compare(GraphObject o1, GraphObject o2) {
			Integer i1 = order.get(o1.getURI());
			if(i1 == null) i1 = 0;
			Integer i2 = order.get(o2.getURI());
			if(i2 == null) i2 = 0;
			return i1.compareTo(i2);
		}
		
	}
	
	private static class UriTablePairs {
		Map<String, Integer> order = new HashMap<String, Integer>();
		Map<String, List<String>> map = new HashMap<String, List<String>>();
		
	}
	
	private UriTablePairs unpackURIResults(ResultSet rs) throws SQLException {

		UriTablePairs utp = new UriTablePairs();
		
		Map<String, List<String>> m = utp.map;
		
		int i = 0;
		
		while(rs.next()) {
			
			String uri = rs.getString(COLUMN_URI);
			
			utp.order.put(uri, i);
			i++;
			
			String table = rs.getString(column_sourceTable);
			
			List<String> list = m.get(table);
			if(list == null) {
				list = new ArrayList<String>();
				m.put(table, list);
			}
			list.add(uri);
			
		}
		
		return utp;
	}

	protected String buildQuery(List<CoreSelectQuery> coreQueries, boolean countQuery) {
		
		StringBuilder outputQuery = new StringBuilder();
		
		String selector = null;
		if(countQuery) {
			if(distinct) {
				selector = "COUNT(DISTINCT " + column_distinct + ")";
			} else {
//				selector = "COUNT(*)";
				selector = "COUNT(DISTINCT " + COLUMN_URI + ")";
			}
		} else if(agg != null) {
			if(aggregationType == AggregationType.average) {
				selector = "AVG";
			} else if(aggregationType == AggregationType.count) {
				selector = "COUNT";
			} else if(aggregationType == AggregationType.max) {
				selector = "MAX";
			} else if(aggregationType == AggregationType.min) {
				selector = "MIN";
			} else if(aggregationType == AggregationType.sum) {
				selector = "SUM";
			} else throw new RuntimeException("Unhandled aggragation type: " + aggregationType);
			
			
			selector += "(";
			
			if(aggregationType == AggregationType.count && agg.isDistinct()) {
				selector += "DISTINCT ";
			}
			
			selector += ( column_aggregation + ")" );
			
		} else if(distinct) {
			selector = "DISTINCT " + column_distinct;
		} else {
			//include distinct
			selector = "*";
		}
		
		outputQuery.append("SELECT " + selector + " FROM ( \n");
		boolean first = true;
		for(CoreSelectQuery csq : coreQueries) {
			
			if(first) {
				first = false;
			} else {
				outputQuery.append("UNION ALL\n");
			}
			outputQuery.append(csq.queryTemplate.toString());
		}
		
		outputQuery.append(") AS T");
		
		if(distinct) {
			outputQuery.append(" WHERE " + column_distinct + " IS NOT NULL");
		}
		
		if(countQuery) {
			
		} else if(agg == null && sq.getDistinct() == false){
			
			String order = "";
			
			boolean hasURISort = false;
			
			if(sortData != null && sortData.size() > 0) {
				for(int i = 0; i < sortData.size(); i++) {
					
					if(i > 0) order += ", ";
					
					SortData sd = sortData.get(i);
					
					if(sd.vsp.getPropertyURI().equals(VitalGraphQueryPropertyCriterion.URI) || sd.vsp.getPropertyURI().equals(VitalSigns.get().getPropertiesRegistry().getPropertyURI(Property_URIProp.class)) ) {
						
						hasURISort = true;
						
						order += ( COLUMN_URI + (sd.vsp.isReverse() ? " DESC" : " ASC") );
						
					} else {
						
						order += ( sd.label + (sd.vsp.isReverse() ? " DESC" : " ASC") );
					}
					
				}
				
			}
			
			if(!hasURISort) {
				if(sortData != null && sortData.size() > 0) order += ", ";
				order += ( COLUMN_URI + " ASC");
			}
			
			outputQuery.append(" ORDER BY " + order + ( sq.getLimit() > 0 ? ( " LIMIT " + sq.getLimit()) : " LIMIT 10000") + " OFFSET " + sq.getOffset());
			
		} else if(agg != null) {
			
		} else if(distinct) {
			
			String distinctSort = sq.getDistinctSort();
			
			if(distinctSort == null) distinctSort = VitalSelectQuery.asc;
			
			int offset = sq.getOffset();
			int limit = sq.getLimit();
			if(limit == 0) limit = 10000;
			
			if(sq.isDistinctFirst() || sq.isDistinctLast()) {
				offset = 0;
				limit = 1;
			}
			
			if(sq.isDistinctLast()) {
				if(distinctSort.equals(VitalSelectQuery.asc)) {
					distinctSort = VitalSelectQuery.desc;
				} else if(distinctSort.equalsIgnoreCase(VitalSelectQuery.desc)) {
					distinctSort = VitalSelectQuery.asc;
				}
			}
			
			outputQuery.append(" ORDER BY " + column_distinct + " " + distinctSort.toUpperCase() + " LIMIT " + limit + " OFFSET " + offset);
			
		}

		return outputQuery.toString();
	}
	
	protected CoreSelectQuery buildSingleSegmentQuery(VitalGraphCriteriaContainer container, SegmentTable table, boolean countQuery) throws SQLException {
		
		String tableName = SQLUtils.escapeID(connection, table.tableName);
		
		StringBuilder outputQuery = new StringBuilder();
		
		CoreSelectQuery c = new CoreSelectQuery();
		c.tableName = tableName;
		c.queryTemplate = outputQuery;
		

		String tableAlias = "parent";
		
		String sortString = "";
		
		String columnAggregation = null;
		
		String columnDistinct = null;
		
		String uriTableNamePart = "DISTINCT " + COLUMN_URI + ", '" + table.tableName + "' AS " + column_sourceTable;
		
		if( ( agg != null && aggregationType == AggregationType.count && aggregationPropertyURI == null )  || ( agg == null && !sq.isDistinct() && sq.getSortProperties() != null && sq.getSortProperties().size() > 0 ) ) {
			
			int i = 0;
			
			boolean updateSortData = false;
			if(sortData == null) {
				updateSortData = true;
				sortData = new ArrayList<SortData>();
			}
			
			for(VitalSortProperty vsp : sq.getSortProperties() ) {
				
				
				String pURI = vsp.getPropertyURI();
				if(VitalSortProperty.INDEXORDER.equals(pURI)) continue;
				if(VitalSortProperty.RELEVANCE.equals(pURI)) throw new RuntimeException("SQL endpoint does not support relevance sorting.");
				
				PropertyMetadata pm = VitalSigns.get().getPropertiesRegistry().getProperty(pURI);
				
				if(pm == null) throw new RuntimeException("Property not found in VitalSigns: " + pURI);
				
				if(pm.isMultipleValues()) throw new RuntimeException("Cannot sort with multivalue property");
				
				Class<? extends IProperty> baseClass = pm.getBaseClass();
				
				PropertyInfo info = class2Info.get(baseClass);
			
				String label = "sortValue" + i;
				
				if(vsp.getPropertyURI().equals(VitalGraphQueryPropertyCriterion.URI) || vsp.getPropertyURI().equals(VitalSigns.get().getPropertiesRegistry().getPropertyURI(Property_URIProp.class)) ) {
					
					//no need to select it
				} else {
					
					sortString += (
						", ( SELECT " + info.singleValueColumn + " FROM " + tableName + " AS X WHERE X." + COLUMN_URI + " = " + tableAlias + "." + COLUMN_URI + " AND " + COLUMN_NAME + " = ? ) AS " + label
					);
					c.substitutes.add(pURI);
					
				}
				
				if(updateSortData) sortData.add(new SortData(vsp, label));
				
			}
			
			i++;
			
		} else if(agg != null) {
			
			PropertyMetadata pm = VitalSigns.get().getPropertiesRegistry().getProperty(aggregationPropertyURI);
			if(pm == null) throw new RuntimeException("Cannot use external property for aggregation: " + aggregationPropertyURI);
			
			String aggregationValueColumn = class2Info.get(pm.getBaseClass()).singleValueColumn;
			

			columnAggregation = 			"(" +
					"SELECT " + aggregationValueColumn + " FROM " + tableName + " AS X WHERE X." + COLUMN_URI + " = " + tableAlias + "." + COLUMN_URI + " AND " + COLUMN_NAME + " = ? " +
					") AS " + column_aggregation;
			
			uriTableNamePart = 
					( countQuery ? "" : "DISTINCT " ) +
					COLUMN_URI + ", " + columnAggregation;
			
			c.substitutes.add(aggregationPropertyURI);
			
		} else if(distinct) {
			
			PropertyMetadata pm = VitalSigns.get().getPropertiesRegistry().getProperty(aggregationPropertyURI);
			if(pm == null) throw new RuntimeException("Cannot use external property for aggregation: " + aggregationPropertyURI);
			
			String aggregationValueColumn = class2Info.get(pm.getBaseClass()).singleValueColumn;
			
			columnDistinct = "( SELECT DISTINCT " + aggregationValueColumn + " FROM " + tableName + " AS X WHERE X." + COLUMN_URI + " = " + tableAlias + "." + COLUMN_URI + " AND " + COLUMN_NAME + " = ? " +
				" AND " + tableAlias + "." + aggregationValueColumn + " IS NOT NULL " + 
			") AS " + column_distinct;
			
			uriTableNamePart = 
					COLUMN_URI + ", " + columnDistinct;
			
			c.substitutes.add(aggregationPropertyURI);
			
		}
		
//		c.queryTemplate.append("SELECT " + uriTableNamePart + sortString + " FROM " + tableName + " AS " + tableAlias + " WHERE ");
		c.tableName = tableName;
		
		
		
		boolean wrappedSelectCount = false;
		
		String selector = null;
		if(countQuery) {
			if(distinct) {
				selector = "COUNT(DISTINCT " + columnDistinct /*column_distinct*/ + ")";
			} else if(columnAggregation != null) {
				wrappedSelectCount = true;
				if(sq.isDistinct()) {
					outputQuery.insert(0, " SELECT COUNT(DISTINCT " + column_aggregation + ") FROM ( ");
				} else {
					outputQuery.insert(0, " SELECT COUNT(DISTINCT " + COLUMN_URI + ") FROM ( ");
				}
				selector = "" + COLUMN_URI + ", " + columnAggregation /*column_distinct*/ + "";
			} else {
//				selector = "COUNT(*)";
				selector = "COUNT(DISTINCT " + COLUMN_URI + ")";
			}
		} else if(agg != null) {
			if(aggregationType == AggregationType.average) {
				selector = "AVG";
			} else if(aggregationType == AggregationType.count) {
				selector = "COUNT";
			} else if(aggregationType == AggregationType.max) {
				selector = "MAX";
			} else if(aggregationType == AggregationType.min) {
				selector = "MIN";
			} else if(aggregationType == AggregationType.sum) {
				selector = "SUM";
			} else throw new RuntimeException("Unhandled aggragation type: " + aggregationType);
			
			
			selector += "(";
			
			if(aggregationType == AggregationType.count && agg.isDistinct()) {
				selector += "DISTINCT ";
			}
			
			selector += ( columnAggregation /*column_aggregation*/ + ")" );
			
		} else if(distinct) {
			selector = "DISTINCT " + columnDistinct /*column_distinct*/;
		} else {
			//include distinct
			selector = "DISTINCT " + COLUMN_URI;
		}
		
		outputQuery.append("SELECT " + selector + sortString+ " FROM " + tableName + " AS " + tableAlias + " WHERE ( \n");
		
		
		
		
		processContainer(c, null, sq.getCriteriaContainer(), "", true);
		
		
		if(distinct) {
			outputQuery.append(" WHERE " + column_distinct + " IS NOT NULL");
		}
		
		outputQuery.append(")");
		
		if(wrappedSelectCount) {
			outputQuery.append(") AS Z WHERE " + column_aggregation + " IS NOT NULL ");
		}
		
		if(countQuery) {
			
		} else if(agg == null && sq.getDistinct() == false){
			
			String order = "";
			
			boolean hasURISort = false;
			
			if(sortData != null && sortData.size() > 0) {
				for(int i = 0; i < sortData.size(); i++) {
					
					if(i > 0) order += ", ";
					
					SortData sd = sortData.get(i);
					
					if(sd.vsp.getPropertyURI().equals(VitalGraphQueryPropertyCriterion.URI) || sd.vsp.getPropertyURI().equals(VitalSigns.get().getPropertiesRegistry().getPropertyURI(Property_URIProp.class)) ) {
						
						hasURISort = true;
						
						order += ( COLUMN_URI + (sd.vsp.isReverse() ? " DESC" : " ASC") );
						
					} else {
						
						order += ( sd.label + (sd.vsp.isReverse() ? " DESC" : " ASC") );
					}
					
				}
				
			}
			
			if(!hasURISort) {
				if(sortData != null && sortData.size() > 0) order += ", ";
				order += ( COLUMN_URI + " ASC");
			}
			
			outputQuery.append(" ORDER BY " + order + ( sq.getLimit() > 0 ? ( " LIMIT " + sq.getLimit()) : " LIMIT 10000") + " OFFSET " + sq.getOffset());
			
		} else if(agg != null) {
			
		} else if(distinct) {
			
			String distinctSort = sq.getDistinctSort();
			
			if(distinctSort == null) distinctSort = VitalSelectQuery.asc;
			
			int offset = sq.getOffset();
			int limit = sq.getLimit();
			if(limit == 0) limit = 10000;
			
			if(sq.isDistinctFirst() || sq.isDistinctLast()) {
				offset = 0;
				limit = 1;
			}
			
			if(sq.isDistinctLast()) {
				if(distinctSort.equals(VitalSelectQuery.asc)) {
					distinctSort = VitalSelectQuery.desc;
				} else if(distinctSort.equalsIgnoreCase(VitalSelectQuery.desc)) {
					distinctSort = VitalSelectQuery.asc;
				}
			}
			
			outputQuery.append(" ORDER BY " + column_distinct + " " + distinctSort.toUpperCase() + " LIMIT " + limit + " OFFSET " + offset);
			
		}
		
		return c;
		
	}

	//this is a core select query
	protected static class CoreSelectQuery {
		
		StringBuilder queryTemplate = new StringBuilder();
		
		//for prepared statement
		List<Object> substitutes = new ArrayList<Object>();

		String tableName;
		
	}
	
	protected CoreSelectQuery prepareCoreQuery(VitalGraphCriteriaContainer criteriaContainer, SegmentTable table, String indent, boolean countQuery) throws SQLException {

		CoreSelectQuery c = new CoreSelectQuery();
		
		String tableName = SQLUtils.escapeID(connection, table.tableName);
		
		String tableAlias = "parent";
		
		String sortString = "";
		
		String uriTableNamePart = "DISTINCT " + COLUMN_URI + ", '" + table.tableName + "' AS " + column_sourceTable;
		
		if( ( agg != null && aggregationType == AggregationType.count && aggregationPropertyURI == null )  || ( agg == null && !sq.isDistinct() && sq.getSortProperties() != null && sq.getSortProperties().size() > 0 ) ) {
			
			int i = 0;
			
			boolean updateSortData = false;
			if(sortData == null) {
				updateSortData = true;
				sortData = new ArrayList<SortData>();
			}
			
			for(VitalSortProperty vsp : sq.getSortProperties() ) {
				
				
				String pURI = vsp.getPropertyURI();
				if(VitalSortProperty.INDEXORDER.equals(pURI)) continue;
				if(VitalSortProperty.RELEVANCE.equals(pURI)) throw new RuntimeException("SQL endpoint does not support relevance sorting.");
				
				PropertyMetadata pm = VitalSigns.get().getPropertiesRegistry().getProperty(pURI);
				
				if(pm == null) throw new RuntimeException("Property not found in VitalSigns: " + pURI);
				
				if(pm.isMultipleValues()) throw new RuntimeException("Cannot sort with multivalue property");
				
				Class<? extends IProperty> baseClass = pm.getBaseClass();
				
				PropertyInfo info = class2Info.get(baseClass);
			
				String label = "sortValue" + i;
				
				if(vsp.getPropertyURI().equals(VitalGraphQueryPropertyCriterion.URI) || vsp.getPropertyURI().equals(VitalSigns.get().getPropertiesRegistry().getPropertyURI(Property_URIProp.class)) ) {
					
					//no need to select it
				} else {
					
					sortString += (
						", ( SELECT " + info.singleValueColumn + " FROM " + tableName + " AS X WHERE X." + COLUMN_URI + " = " + tableAlias + "." + COLUMN_URI + " AND " + COLUMN_NAME + " = ? ) AS " + label
					);
					c.substitutes.add(pURI);
					
				}
				
				if(updateSortData) sortData.add(new SortData(vsp, label));
				
			}
			
			i++;
			
		} else if(agg != null) {
			
			PropertyMetadata pm = VitalSigns.get().getPropertiesRegistry().getProperty(aggregationPropertyURI);
			if(pm == null) throw new RuntimeException("Cannot use external property for aggregation: " + aggregationPropertyURI);
			
			String aggregationValueColumn = class2Info.get(pm.getBaseClass()).singleValueColumn;
			
			uriTableNamePart = 
					( countQuery ? "" : "DISTINCT " ) +
					COLUMN_URI + ", " + 
			"(" +
				"SELECT " + aggregationValueColumn + " FROM " + tableName + " AS X WHERE X." + COLUMN_URI + " = " + tableAlias + "." + COLUMN_URI + " AND " + COLUMN_NAME + " = ? " +
			") AS " + column_aggregation;
			
			c.substitutes.add(aggregationPropertyURI);
			
		} else if(distinct) {
			
			PropertyMetadata pm = VitalSigns.get().getPropertiesRegistry().getProperty(aggregationPropertyURI);
			if(pm == null) throw new RuntimeException("Cannot use external property for aggregation: " + aggregationPropertyURI);
			
			String aggregationValueColumn = class2Info.get(pm.getBaseClass()).singleValueColumn;
			
			uriTableNamePart = 
					COLUMN_URI + ", " + 
			"( SELECT DISTINCT " + aggregationValueColumn + " FROM " + tableName + " AS X WHERE X." + COLUMN_URI + " = " + tableAlias + "." + COLUMN_URI + " AND " + COLUMN_NAME + " = ? " +
						" AND " + tableAlias + "." + aggregationValueColumn + " IS NOT NULL " + 
			") AS " + column_distinct;
			
			c.substitutes.add(aggregationPropertyURI);
			
		}
		
		c.queryTemplate.append("SELECT " + uriTableNamePart + sortString + " FROM " + tableName + " AS " + tableAlias + " WHERE ");
		c.tableName = tableName;
		processContainer(c, null, criteriaContainer, indent, true);
		
		return c;
		
	}

	protected void processContainer(CoreSelectQuery c, VitalGraphCriteriaContainer parent, VitalGraphCriteriaContainer criteriaContainer, String indent, boolean first) {

		//open container
		c.queryTemplate.append(indent);
		
		if(!first) {
			c.queryTemplate.append(parent.getType() == QueryContainerType.and ? " AND " : " OR ");
		}
		
		c.queryTemplate.append("(\n");

		boolean innerFirst = true;
		for(VitalGraphQueryElement el : criteriaContainer) {
			if(el instanceof VitalGraphCriteriaContainer) {
				processContainer(c, criteriaContainer, (VitalGraphCriteriaContainer) el, indent + "    ", innerFirst);
				innerFirst = false;
			} else if(el instanceof VitalGraphQueryPropertyCriterion){
				processCriterion(c, criteriaContainer, (VitalGraphQueryPropertyCriterion)el, indent + "    ", innerFirst, false, null, false);
				innerFirst = false;
			}
		}

		//close container
		c.queryTemplate.append(indent).append(")\n");
	}
	
	static class PropertyInfo {
		String singleValueColumn;
		String multiValueColumn;
		List<Class> validValueClasses;
		List<Comparator> supportedComparators;
		public PropertyInfo(String singleValueColumn, String multiValueColumn,
				Class[] validValueClasses, Comparator[] comparators) {
			super();
			this.singleValueColumn = singleValueColumn;
			this.multiValueColumn = multiValueColumn;
			this.validValueClasses = Arrays.asList(validValueClasses);
			this.supportedComparators = Arrays.asList(comparators);
		}
		
	}
	static Map<Class<? extends IProperty>, PropertyInfo> class2Info = new HashMap<Class<? extends IProperty>, PropertyInfo>();
	static {
		class2Info.put(BooleanProperty.class, new PropertyInfo(COLUMN_VALUE_BOOLEAN, COLUMN_VALUE_BOOLEAN_MULTIVALUE, new Class[]{Boolean.class},
				new Comparator[]{Comparator.EXISTS, Comparator.EQ, Comparator.NOT_EXISTS, Comparator.NE, Comparator.ONE_OF, Comparator.NONE_OF}));
		class2Info.put(DateProperty.class, new PropertyInfo(COLUMN_VALUE_DATE, COLUMN_VALUE_DATE_MULTIVALUE, new Class[]{Date.class},
				new Comparator[]{Comparator.EXISTS, Comparator.EQ, Comparator.NOT_EXISTS, Comparator.NE, Comparator.ONE_OF, Comparator.NONE_OF, Comparator.GE, Comparator.GT, Comparator.LE, Comparator.LT}));
		class2Info.put(DoubleProperty.class, new PropertyInfo(COLUMN_VALUE_DOUBLE, COLUMN_VALUE_DOUBLE_MULTIVALUE, new Class[]{Number.class},
				new Comparator[]{Comparator.EXISTS, Comparator.EQ, Comparator.NOT_EXISTS, Comparator.NE, Comparator.ONE_OF, Comparator.NONE_OF, Comparator.GE, Comparator.GT, Comparator.LE, Comparator.LT}));
		class2Info.put(FloatProperty.class, new PropertyInfo(COLUMN_VALUE_FLOAT, COLUMN_VALUE_FLOAT_MULTIVALUE, new Class[]{Number.class},
				new Comparator[]{Comparator.EXISTS, Comparator.EQ, Comparator.NOT_EXISTS, Comparator.NE, Comparator.ONE_OF, Comparator.NONE_OF, Comparator.GE, Comparator.GT, Comparator.LE, Comparator.LT}));
		class2Info.put(GeoLocationProperty.class, new PropertyInfo(COLUMN_VALUE_GEOLOCATION, COLUMN_VALUE_GEOLOCATION_MULTIVALUE, new Class[]{GeoLocationProperty.class},
				new Comparator[]{}));
		class2Info.put(IntegerProperty.class, new PropertyInfo(COLUMN_VALUE_INTEGER, COLUMN_VALUE_INTEGER_MULTIVALUE, new Class[]{Number.class},
				new Comparator[]{Comparator.EXISTS, Comparator.EQ, Comparator.NOT_EXISTS, Comparator.NE, Comparator.ONE_OF, Comparator.NONE_OF, Comparator.GE, Comparator.GT, Comparator.LE, Comparator.LT}));
		class2Info.put(LongProperty.class, new PropertyInfo(COLUMN_VALUE_LONG, COLUMN_VALUE_LONG_MULTIVALUE, new Class[]{Number.class},
				new Comparator[]{Comparator.EXISTS, Comparator.EQ, Comparator.NOT_EXISTS, Comparator.NE, Comparator.ONE_OF, Comparator.NONE_OF, Comparator.GE, Comparator.GT, Comparator.LE, Comparator.LT}));
		class2Info.put(OtherProperty.class, new PropertyInfo(COLUMN_VALUE_OTHER, COLUMN_VALUE_OTHER_MULTIVALUE, new Class[]{Number.class}, 
				new Comparator[]{}));
		class2Info.put(StringProperty.class, new PropertyInfo(COLUMN_VALUE_STRING, COLUMN_VALUE_STRING_MULTIVALUE, new Class[]{String.class}, 
				new Comparator[]{Comparator.EXISTS, Comparator.EQ, Comparator.NOT_EXISTS, Comparator.NE, Comparator.ONE_OF, Comparator.NONE_OF, Comparator.CONTAINS_CASE_SENSITIVE, Comparator.CONTAINS_CASE_INSENSITIVE, Comparator.EQ_CASE_INSENSITIVE,
				Comparator.REGEXP, Comparator.REGEXP_CASE_SENSITIVE}));
		class2Info.put(TruthProperty.class, new PropertyInfo(COLUMN_VALUE_TRUTH, COLUMN_VALUE_TRUTH_MULTIVALUE, new Class[]{Truth.class},
				new Comparator[]{Comparator.EXISTS, Comparator.EQ, Comparator.NOT_EXISTS, Comparator.NE, Comparator.ONE_OF, Comparator.NONE_OF}));
		class2Info.put(URIProperty.class, new PropertyInfo(COLUMN_VALUE_URI, COLUMN_VALUE_URI_MULTIVALUE, new Class[]{String.class}, 
				new Comparator[]{Comparator.EXISTS, Comparator.EQ, Comparator.NOT_EXISTS, Comparator.NE, Comparator.ONE_OF, Comparator.NONE_OF}));
	}

	void processCriterion(CoreSelectQuery c,
			VitalGraphCriteriaContainer parentContainer,
			VitalGraphQueryPropertyCriterion el, String indent, boolean innerFirst, boolean singleCriterionQueries, TypeANDResponse typePropTypes, boolean probing) {

		
		if(!singleCriterionQueries) {
			
			c.queryTemplate.append(indent);
			if(!innerFirst) {
				c.queryTemplate.append(parentContainer.getType() == QueryContainerType.and ? " AND " : " OR ");
			}
			
			c.queryTemplate.append("(" );
			
		}
		
		
		String propertyURI = null;
		
		Object value = null;
		
		Comparator comparator = el.getComparator();
		
		String valueColumn = null;
		
		if(el != SQLSingleSegmentQueryHandler.EMPTY_CRITERION) {
			
			if(el instanceof VitalGraphQueryTypeCriterion) {
				
				VitalGraphQueryTypeCriterion t = (VitalGraphQueryTypeCriterion) el;
	
				propertyURI = VitalCoreOntology.vitaltype.getURI();
				
	//			valueColumn = VitalSignsToSqlBridge.COLUMN_VALUE_URI_MULTIVALUE;
				valueColumn = VitalSignsToSqlBridge.COLUMN_VITALTYPE;
				
				if(comparator == Comparator.ONE_OF || comparator == Comparator.NONE_OF) {
					
					if(t.isExpandTypes()) throw new RuntimeException("Cannot expand type and use oneof/noneof comparator");
					
					value = el.getValue();
					
					if(!(value instanceof Collection)) throw new RuntimeException("ONE_OF/NONE_OF value must be a collection");
					
					List<Object> classes = new ArrayList<Object>();
					
					for(Object v : (Collection)value) {
	
						if(v instanceof Class) {
							String classURI = VitalSigns.get().getClassesRegistry().getClassURI((Class<? extends GraphObject>) v);
							classes.add(classURI);
						} else {
							classes.add(v);
						}
						
					}
					
					value = classes;
						
					
				} else if( comparator == Comparator.EQ || comparator == Comparator.NE ){
					
					String classURI = VitalSigns.get().getClassesRegistry().getClassURI(t.getType());
					
					if(t.isExpandTypes()) {
						
						Set<String> set = type2ExpandedTypes.get(classURI);
						if(set == null) throw new RuntimeException("Type not expanded in initial phase: " + classURI);
						comparator = comparator == Comparator.NE  ? Comparator.NONE_OF : Comparator.ONE_OF;
						value = set;
						
					} else {
						
						value = classURI;
						
					}
					
					
				}	
				
				
			} else {
				
				propertyURI = el.getPropertyURI();
				
				value = el.getValue();
				
			}
			
			
			if(value == null) {
				
				if(comparator != Comparator.EXISTS && comparator != Comparator.NOT_EXISTS) {
					throw new RuntimeException("Value is required when comparator is different from EXISTS/NOT_EXISTS");
				}
			
			} else {
				
				if(value instanceof IProperty) value = ((IProperty)value).rawValue();
				
			}
			
			if(VitalGraphQueryPropertyCriterion.URI.equals(propertyURI)) {
				propertyURI = VitalSigns.get().getPropertiesRegistry().getPropertyURI(Property_URIProp.class);
			}
			
			PropertyMetadata pm = VitalSigns.get().getPropertiesRegistry().getProperty(propertyURI);
			
			boolean mv = false;
			
			Class<? extends IProperty> baseClass = null;
			
			if(pm != null) {
				
				mv = pm.isMultipleValues();
				
				baseClass = pm.getBaseClass();
				
			} else {
				
				//determine base class from input value ??
				mv = ( comparator == Comparator.CONTAINS  || comparator == Comparator.NOT_CONTAINS ) ;
				
//				throw new RuntimeException("External properties queries are not supported yet.");
				
				if(value == null) {
					//exists/not_exists case
				} else if(value instanceof IProperty) {
					baseClass = ((IProperty)value).unwrapped().getClass();
				} else {
					if(value instanceof Boolean) {
						baseClass = BooleanProperty.class;
					} else if(value instanceof Date) {
						baseClass = DateProperty.class;
					} else if(value instanceof Double) {
						baseClass = DoubleProperty.class;
					} else if(value instanceof Float) {
						baseClass = FloatProperty.class;
					} else if(value instanceof Integer) {
						baseClass = IntegerProperty.class;
					} else if(value instanceof Long) {
						baseClass = LongProperty.class;
					} else if(value instanceof String) {
						baseClass = StringProperty.class;
					} else if(value instanceof Truth) {
						baseClass = TruthProperty.class;
					} else {
						throw new RuntimeException("Couldn't map the value of class " + value.getClass().getCanonicalName() + " to any column");
					}
				}
				
				
			}
				
			PropertyInfo info = class2Info.get(baseClass);
			
			if(el instanceof VitalGraphQueryTypeCriterion) {
				valueColumn = COLUMN_VITALTYPE;
			}else if(mv) {
				valueColumn = info.multiValueColumn;
			} else {
				valueColumn = info.singleValueColumn;
			}
			
			
			if(mv && ( comparator == Comparator.CONTAINS  || comparator == Comparator.NOT_CONTAINS ) ) {
				
			} else {
				if(!info.supportedComparators.contains(comparator)) throw new RuntimeException("Unsupported comparator: " + comparator + " property base type: " + baseClass.getSimpleName());
			}
			
			
			if(comparator == Comparator.NONE_OF || comparator == Comparator.ONE_OF) {
				if(!(value instanceof Collection)) throw new RuntimeException("NONE_OF/ONE_OF value must be a collection");
			}
			
		}
		
		

		boolean notIn = false;
		
		String valuePart = null;
		
		List<Object> vSubstitutes = new ArrayList<Object>();
		
		if(el != SQLSingleSegmentQueryHandler.EMPTY_CRITERION && ( !probing || el instanceof VitalGraphQueryTypeCriterion) ) {
			
//				c.queryTemplate.append(COLUMN_NAME).append(" = ")
			if(comparator == Comparator.CONTAINS) {
				
				valuePart = " " + valueColumn + " = ?";
	
				vSubstitutes.add(value);
				
			} else if(comparator == Comparator.CONTAINS_CASE_INSENSITIVE) {
				
				String locate = dataSource.getDialect().locate("?", "LOWER(" + valueColumn + ")");
				
				valuePart = " " + locate + " " + ( !el.isNegative() ? ">" : "=" ) + " 0 ";
				
				vSubstitutes.add(value.toString().toLowerCase());
				
			} else if(comparator == Comparator.CONTAINS_CASE_SENSITIVE) {
				
				String locate = dataSource.getDialect().locate("?", valueColumn);
				
				valuePart = " " + locate + " " + ( !el.isNegative() ? ">" : "=" ) + " 0 ";
				
				vSubstitutes.add(value);
				
			} else if(comparator == Comparator.EQ) {
				
				valuePart = " " + valueColumn + " = ? ";
				
				vSubstitutes.add(value);
				
			} else if(comparator == Comparator.EQ_CASE_INSENSITIVE) {
				
				valuePart = " LOWER(" + valueColumn + ") = ? ";
				
				vSubstitutes.add(value.toString().toLowerCase());
				
			} else if(comparator == Comparator.EXISTS) {
				
				valuePart = "";
				
			} else if(comparator == Comparator.GE) {
				
				valuePart = " " + valueColumn + " >= ? ";
				
				vSubstitutes.add(value);
				
			} else if(comparator == Comparator.GT) {
				
				valuePart = " " + valueColumn + " > ? ";
				
				vSubstitutes.add(value);
				
			} else if(comparator == Comparator.LE) {
				
				valuePart = " " + valueColumn + " <= ? ";
				
				vSubstitutes.add(value);
				
			} else if(comparator == Comparator.LT) {
				
				valuePart = " " + valueColumn + " < ? ";
				
				vSubstitutes.add(value);
				
			} else if(comparator == Comparator.NE) {
				
				valuePart = " " + valueColumn + " != ? ";
				
				vSubstitutes.add(value);
				
			} else if(comparator == Comparator.NONE_OF || comparator == Comparator.ONE_OF) {
				
				valuePart = " " + valueColumn + (comparator == Comparator.ONE_OF ? "" : " NOT") + " IN (";
				
				Collection<?> values = (Collection<?>) value;
				boolean first = true;
				for(Object v : values) {
					if(first) {
						valuePart += "?";
						first = false; 
					} else {
						valuePart += ", ?";
					}
					if(v instanceof IProperty) v = ((IProperty)v).rawValue();
					vSubstitutes.add(v);
				}
				
				valuePart += ")";
				
				
			} else if(comparator == Comparator.NOT_CONTAINS) {
				
				notIn = true;
				
				valuePart = " " + valueColumn + " = ? ";
				
				vSubstitutes.add(value);
				
			} else if(comparator == Comparator.NOT_EXISTS) {
				
				notIn = true;
				
				valuePart = "";
				
			} else if(comparator == Comparator.REGEXP) {
				
				String regexp = dataSource.getDialect().regexp("?", "LOWER(" + valueColumn + ")");
				
				valuePart = " " + regexp + " ";//LOWER(" + valueColumn + ") REGEXP ? ";
				
	//			vSubstitutes.add(value.toString().toLowerCase());
				vSubstitutes.add(value.toString().toLowerCase());
				
			} else if(comparator == Comparator.REGEXP_CASE_SENSITIVE) {
				
				//http://stackoverflow.com/questions/3153912/case-sensitive-rlike
	//			valuePart = " AND CAST(" + valueColumn + " as BINARY) REGEXP ? ";
				//utf8 binary collation now
				String regexp = dataSource.getDialect().regexp("?", valueColumn);
				valuePart = " " + regexp + " "; //+ valueColumn + " REGEXP ? ";
				
				vSubstitutes.add(value);
				
			} else {
				
				throw new RuntimeException("Unhandled comparator: " + comparator);
				
			}
			
			
		
		}
		
		//this is common
		if(singleCriterionQueries) {
			
			c.queryTemplate.append("SELECT DISTINCT " + COLUMN_URI + " FROM " + c.tableName + " WHERE ");
			
		} else {
			
			c.queryTemplate.append(COLUMN_URI).append((notIn ? " NOT" : "") + " IN ( ").append("SELECT " + COLUMN_URI + " FROM " + c.tableName + " WHERE ");
			
		}
		
		
		boolean insertAnd = false;
		
		if(typePropTypes != null) {
			
			
			if(typePropTypes.types.size() > 0) {
				
				c.queryTemplate.append(COLUMN_VITALTYPE);
				
				if(typePropTypes.types.size() > 1 ) {
					
					if( ! typePropTypes.inValues) c.queryTemplate.append(" NOT ");
					c.queryTemplate.append(" IN ( ");
					
					boolean first = true;
					for(String tURI : typePropTypes.types) {
						if(first) {
							first = false;
						} else {
							c.queryTemplate.append(", ");
						}
						c.queryTemplate.append("?");
						
						c.substitutes.add(tURI);
						
					}
					
					c.queryTemplate.append(" )");
					

				} else {
					
					c.queryTemplate.append( typePropTypes.inValues ? " = " : " != ").append("?");
					
					c.substitutes.add(typePropTypes.types.get(0));
					
				}
				
				insertAnd = true;
				
				
			}
			
			
			Comparator chC = typePropTypes.channelURIComparator;
			
			if(chC != null) {
				
				if(insertAnd) {
					c.queryTemplate.append(" AND ");
				}
				
				c.queryTemplate.append(COLUMN_CHANNEL_URI);
				
				if(chC == Comparator.ONE_OF || chC == Comparator.NONE_OF) {
					if( chC == Comparator.NONE_OF ) c.queryTemplate.append(" NOT ");
					 c.queryTemplate.append(" IN (");
					 
					 boolean first = true;
					 for(String channelURI : typePropTypes.channelURIs) {
						 if(first) {
							 first = false;
						 } else {
							 c.queryTemplate.append(", ");
						 }
						 c.queryTemplate.append("?");
						 c.substitutes.add(channelURI);
					 }
					 
					 c.queryTemplate.append(" )");
					 
				} else if(chC == Comparator.EQ || chC == Comparator.NE) {
					
					c.queryTemplate.append(chC == Comparator.EQ ? " = " : " != ").append("?");
					
					c.substitutes.add(typePropTypes.channelURIs.iterator().next());
			
					
				}
				
				insertAnd = true;
				
				
				
			}
				
			if(typePropTypes.minTimestamp != null) {
				
				if(insertAnd) {
					c.queryTemplate.append(" AND ");
				}
				
				c.queryTemplate.append(COLUMN_TSTAMP)
					.append(typePropTypes.minTimestampInclusive ? " >= " : " > ")
					.append("?");
				
				c.substitutes.add(typePropTypes.minTimestamp);
				
				insertAnd = true;
				
			}
			
			if(typePropTypes.maxTimestamp != null) {
				
				if(insertAnd) {
					c.queryTemplate.append(" AND ");
				}
				
				c.queryTemplate.append(COLUMN_TSTAMP)
					.append(typePropTypes.maxTimestampInclusive ? " <= " : " < ")
					.append("?");
				
				c.substitutes.add(typePropTypes.maxTimestamp);
				
				insertAnd = true;
			
				
			}
			
		}
			
		
		//shorthand for 
		if(el instanceof VitalGraphQueryTypeCriterion) {

		} else if(el != SQLSingleSegmentQueryHandler.EMPTY_CRITERION){
			
			if(insertAnd) {
				c.queryTemplate.append(" AND ");
			}
			
			c.queryTemplate.append(COLUMN_NAME);
			if(!el.isExpandProperty()) {
				c.queryTemplate.append(" = ?");
				c.substitutes.add(propertyURI);
			} else {
				
				Set<String> set = property2ExpandedProperties.get(propertyURI);
				if(set == null) throw new RuntimeException("Property not expanded in initial phase: " + propertyURI);
				
				//expand property 
				c.queryTemplate.append(" IN (");
				boolean first = true;
				for(String pURI : set) {
					if(first) {
						first = false;
						c.queryTemplate.append("?");
					} else {
						c.queryTemplate.append(", ?");
					}
					c.substitutes.add(pURI);
				}
				c.queryTemplate.append(")");
			}
			
		}
		
		
		if(el != SQLSingleSegmentQueryHandler.EMPTY_CRITERION && (!probing || el instanceof VitalGraphQueryTypeCriterion)) {
			if(!(el instanceof VitalGraphQueryTypeCriterion)) {
				if(valuePart.length() > 0) {
					c.queryTemplate.append(" AND");
				}
			}
			c.queryTemplate.append(valuePart);
			c.substitutes.addAll(vSubstitutes);
		}
		
		if(singleCriterionQueries) {
			
		} else {
			
			c.queryTemplate.append(")");
			
			c.queryTemplate.append(indent).append(")\n");
			
		}
		
	}

	/**
	 * this filter performs the following transformations:
	 * - all negated comparators are turned into positive
	 * - 
	 */
	private void queryPreprocessor1(VitalGraphCriteriaContainer criteriaContainer) {

		for( int i = 0 ; i < criteriaContainer.size(); i++ ) {
		
			VitalGraphQueryElement el = criteriaContainer.get(i);
			
			if(el instanceof VitalGraphCriteriaContainer) {
			
				queryPreprocessor1((VitalGraphCriteriaContainer) el);
				
			} else if(el instanceof VitalGraphQueryPropertyCriterion){
				
				VitalGraphQueryPropertyCriterion pc = (VitalGraphQueryPropertyCriterion) el;
				
				Comparator c = pc.getComparator();
				
				if(c == null) throw new RuntimeException("No criterion comparator");
				
				if(unsupportedComparatorsSet.contains(c)) {
					throw new RuntimeException("Criterion comparator is not supported: " + c);
				}
				
				
				/*
				if(c == Comparator.ONE_OF || c == Comparator.NONE_OF) {
					
					if(pc.isNegative()) throw new RuntimeException("Cannot negate NONE_OF or ONE_OF comparator");
					
					boolean none = pc.getComparator() == Comparator.NONE_OF;

					VitalGraphCriteriaContainer newContainer = new VitalGraphCriteriaContainer(none ? QueryContainerType.and : QueryContainerType.or);
					
					Object val = pc.getValue();
					
					if(!(val instanceof Collection)) throw new RuntimeException("Expected collection for comparator: " + pc.getComparator());
					
					for(Object v : (Collection<?>)val) {
						
						VitalGraphQueryPropertyCriterion c2 = null;
						
						if(pc instanceof VitalGraphQueryTypeCriterion) {
							
							VitalGraphQueryTypeCriterion tc = (VitalGraphQueryTypeCriterion) pc;
							
							c2 = new VitalGraphQueryTypeCriterion(tc.getType());
							
						} else {
							
							c2 = new VitalGraphQueryPropertyCriterion(pc.getPropertyURI());
							
							
						}
						
						
//						if(VitalGraphQueryPropertyCriterion.URI.equals(crit.getPropertyURI())) {
							c2.setSymbol(pc.getSymbol());
							c2.setComparator(none ? Comparator.NE : Comparator.EQ);
							c2.setValue(v);
							c2.setExternalProperty(pc.getExternalProperty());
//						} else {
//							c2
//						}
						
							newContainer.add(c2);
							
					}
					
					criteriaContainer.set(i, newContainer);
					
					continue;
					
					
				}
				*/
				
				
				String propURI = pc.getPropertyURI();
				
				PropertyMetadata pm = null;
				if(!(pc instanceof VitalGraphQueryTypeCriterion)) {
					
					pm = VitalSigns.get().getPropertiesRegistry().getProperty(propURI);
					
					if(pm != null) {
						
						if(pm.isMultipleValues()) {
							
							if(pm.getBaseClass() == StringProperty.class && ( c == Comparator.CONTAINS_CASE_INSENSITIVE || c == Comparator.CONTAINS_CASE_SENSITIVE)) {
							} else if( c == Comparator.CONTAINS || c == Comparator.NOT_CONTAINS ) {
							} else {
								throw new RuntimeException("Multivalue properties may only be queried with CONTAINS/NOT-CONTAINS comparators, or it it's a string multi value property: " + Comparator.CONTAINS_CASE_INSENSITIVE + " or " + Comparator.CONTAINS_CASE_INSENSITIVE);
							}
						}
						
						if( ( c == Comparator.CONTAINS || c == Comparator.NOT_CONTAINS) && !pm.isMultipleValues() ) {
							throw new RuntimeException("CONTAINS/NOT-CONTAINS may only be used for multi-value properties");
						}
						
					}
				}
				
				
				if(pc.isNegative()) {
					
					if(nonNegableComparatorsSet.contains(c)) {
						
					} else {
						
						Comparator newComparator = negatedComparatorsMap.get(c);
						
						if(newComparator == null ) {
							throw new RuntimeException("No corresponding comparator for " + c);
						} else {
							pc.setComparator(newComparator);
							pc.setNegative(false);
						}
						
					}
					
				}
				
			} else {
				
				throw new RuntimeException("unexpected object type");
				
			}
					
			
		
		}

		
	}
	
	private void queryPreprocessor2(
			VitalGraphCriteriaContainer criteriaContainer) {


		for( int i = 0 ; i < criteriaContainer.size(); i++ ) {
		
			VitalGraphQueryElement el = criteriaContainer.get(i);
			
			if(el instanceof VitalGraphCriteriaContainer) {
			
				queryPreprocessor2((VitalGraphCriteriaContainer) el);
				
			} else if(el instanceof VitalGraphQueryPropertyCriterion) {
				
				VitalGraphQueryPropertyCriterion pc = (VitalGraphQueryPropertyCriterion) el;
				
				if(pc instanceof VitalGraphQueryTypeCriterion) {
					
					
					//XXX consider converting it into ONE_OF
					
					//expand types
					VitalGraphQueryTypeCriterion tc = (VitalGraphQueryTypeCriterion) pc;
					
					if(tc.isExpandTypes()) {
						
						
						Class<? extends GraphObject> gType = tc.getType();
						
						if(gType == null) throw new RuntimeException("No class set in type criterion" + tc);
						
						ClassMetadata cm = VitalSigns.get().getClassesRegistry().getClassMetadata(gType);
						if(cm == null) throw new RuntimeException("Class metadata not found for type: " + gType.getCanonicalName());
						
						List<ClassMetadata> subclasses = VitalSigns.get().getClassesRegistry().getSubclasses(cm, true);
						
						if(subclasses.size() > 1) {
							
//							//only in this case add new container
//							VitalGraphCriteriaContainer newContainer = new VitalGraphCriteriaContainer(QueryContainerType.or);
//							
//							for(ClassMetadata m : subclasses) {
//								
//								VitalGraphQueryTypeCriterion nt = new VitalGraphQueryTypeCriterion(GraphElement.Source, m.getClazz());
//								
//								newContainer.add(nt);
//								
//							}
//							
//							criteriaContainer.set(i, newContainer);

							Set<String> s = new HashSet<String>();
							for(ClassMetadata c : subclasses) {
								s.add(c.getURI());
							}
							
							type2ExpandedTypes.put(cm.getURI(), s);
							
						}
						
						
					}
					
					continue;
					
				}
				
				
				//check if it's expand properties case
				if(pc.getExpandProperty()) {
					
					if(pc instanceof VitalGraphQueryTypeCriterion) {
						throw new RuntimeException("VitalGraphQueryTypeCriterion must not have expandProperty flag set!");
					}
					
					String propURI = pc.getPropertyURI();
					
					PropertyMetadata pm = VitalSigns.get().getPropertiesRegistry().getProperty(propURI);
					
					if(pm == null) throw new RuntimeException("Property metadata not found: " + propURI);
					
					Set<String> expandedProperty = null;
					
					List<PropertyMetadata> subProperties = VitalSigns.get().getPropertiesRegistry().getSubProperties(pm, true);
					
					if(subProperties.size() > 1) {
						
						expandedProperty = new HashSet<String>();
						
						for(PropertyMetadata p : subProperties) {

							expandedProperty.add(p.getURI());
							
						}
						
					}
						
					
					if(expandedProperty != null) {
						
						property2ExpandedProperties.put(pm.getURI(), expandedProperty);
						
//						VitalGraphCriteriaContainer newContainer = new VitalGraphCriteriaContainer(QueryContainerType.or);
//						
//						for(String pURI : expandedProperty) {
//							
//							VitalGraphQueryPropertyCriterion c2 = new VitalGraphQueryPropertyCriterion(pURI);
//							
//							c2.setSymbol(pc.getSymbol());
//							c2.setComparator(pc.getComparator());
//							c2.setValue(pc.getValue());
//							c2.setExternalProperty(false);
//							
//							newContainer.add(c2);
//								
//						}
//						
//						criteriaContainer.set(i, newContainer);
						
					}
					
				}
				
			} 
			
		}
		
	}
	
	
	//get rid of single element containers
	void queryProcessor3(VitalGraphCriteriaContainer parent, VitalGraphCriteriaContainer thisContainer) {
		
		List<VitalGraphCriteriaContainer> childContainers = new ArrayList<VitalGraphCriteriaContainer>();
		
		int criteria = 0;
		
		for(VitalGraphQueryElement el : thisContainer) {
			
			if(el instanceof VitalGraphCriteriaContainer) {
				
				childContainers.add((VitalGraphCriteriaContainer) el);
				
			} else if(el instanceof VitalGraphQueryPropertyCriterion) {
				
				criteria++;
				
			}
			
		}
		
		VitalGraphCriteriaContainer newParent = thisContainer;
		
		if(parent != null && criteria == 0 && childContainers.size() == 1) {
			
			parent.add(childContainers.get(0));
			parent.remove(thisContainer);
			
			newParent = parent;
			
		}
		
		for(VitalGraphCriteriaContainer child : childContainers) {
			
			queryProcessor3(newParent, child);
			
		}
		
	}
	
	//get rid of single element containers
	void queryProcessor4(VitalGraphCriteriaContainer parent, VitalGraphCriteriaContainer thisContainer) {
		
		
		List<VitalGraphCriteriaContainer> childContainers = new ArrayList<VitalGraphCriteriaContainer>();
		
		//first process all child containers
		for(VitalGraphQueryElement el : thisContainer) {
			if(el instanceof VitalGraphCriteriaContainer) {
				childContainers.add((VitalGraphCriteriaContainer) el);
			}
		}
		
		
		for(VitalGraphCriteriaContainer child : childContainers) {
			queryProcessor4(thisContainer, child);
		}

		
		//check if this container has only 1 child
		if(parent != null && thisContainer.size() == 1) {
			int indexOf = parent.indexOf(thisContainer);
			parent.set(indexOf, thisContainer.get(0));
		}
		
	}
}
