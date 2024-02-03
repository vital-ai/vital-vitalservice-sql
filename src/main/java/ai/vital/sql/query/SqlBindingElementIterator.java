package ai.vital.sql.query;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ai.vital.sql.connector.VitalSqlDataSource;
import ai.vital.sql.model.SegmentTable;
import ai.vital.vitalservice.VitalStatus;
import ai.vital.vitalservice.query.QueryStats;
import ai.vital.vitalservice.query.ResultList;
import ai.vital.vitalservice.query.VitalGraphCriteriaContainer;
import ai.vital.vitalservice.query.VitalGraphQueryPropertyCriterion;
import ai.vital.vitalservice.query.VitalSelectQuery;
import ai.vital.vitalsigns.model.GraphObject;
import ai.vital.vitalsigns.model.VITAL_Edge;
import ai.vital.vitalsigns.model.VITAL_HyperEdge;
import ai.vital.vitalsigns.model.property.URIProperty;
import ai.vital.vitalsigns.ontology.VitalCoreOntology;
import ai.vital.vitalsigns.query.graph.Arc;
import ai.vital.vitalsigns.query.graph.BindingEl;

//should use same connection instance
public class SqlBindingElementIterator implements Iterator<BindingEl>{

	private final static Logger log = LoggerFactory.getLogger(SqlBindingElementIterator.class);
	
	private List<SegmentTable> segments;
	
	private Arc arc;

	private VitalSelectQuery connectorQuery;
	
	private VitalSelectQuery rootQuery;
	
	List<BindingEl> resultsPage = null;
	
	int resultsIndex = 0;
	
	static int PAGE_SIZE = 1000;
	
	Connection connection;

	private VitalSqlDataSource dataSource;
	
	boolean noMorePages = false; 
	
	SQLGraphObjectResolver resolver;

	private QueryStats queryStats;
	
	/**
	 * @param segments
	 * @param arc
	 * @param parent
	 */
	public SqlBindingElementIterator(VitalSqlDataSource dataSource, Connection connection, List<SegmentTable> segments, Arc arc, GraphObject parent, SQLGraphObjectResolver resolver, QueryStats queryStats) {

		this.dataSource = dataSource;
		
		this.connection = connection;
		
		this.arc = arc;
		
		this.segments = segments;

		this.resolver = resolver;
		
		this.queryStats = queryStats;
		
		if(! arc.isTopArc() ) {
			this.connectorQuery = VitalSelectQuery.createInstance();
			connectorQuery.setOffset(0);
			connectorQuery.setLimit(PAGE_SIZE);
			if(arc.connectorContainer.connectorCriteria > 0 ) {
				connectorQuery.getCriteriaContainer().add( arc.connectorContainer.container );
			}
			
			
			String connectorProperty = null;
			
			if(arc.isHyperArc()) {
				
				if(arc.isForwardNotReverse()) {
					connectorProperty = VitalCoreOntology.hasHyperEdgeSource.getURI();
				} else {
					connectorProperty = VitalCoreOntology.hasHyperEdgeDestination.getURI();
				}
				
			} else {
				
				if( arc.isForwardNotReverse() ) {
					connectorProperty = VitalCoreOntology.hasEdgeSource.getURI();
				} else {
					connectorProperty = VitalCoreOntology.hasEdgeDestination.getURI();
					
				}
				
			}
			
			connectorQuery.getCriteriaContainer().add(new VitalGraphQueryPropertyCriterion(connectorProperty).equalTo(URIProperty.withString(parent.getURI())));
			
		} else {
			
			
			this.rootQuery = VitalSelectQuery.createInstance();
			rootQuery.setOffset(0);
			rootQuery.setLimit(PAGE_SIZE);
			
			VitalGraphCriteriaContainer endpointCriteriaContainer = rootQuery.getCriteriaContainer();
			
			if( arc.endpointContainer.endpointCriteria > 0 ) {
				endpointCriteriaContainer.add(arc.endpointContainer.container);
			}
			
			//select all?
			if(endpointCriteriaContainer.size() == 0 ){
				endpointCriteriaContainer.add(new VitalGraphQueryPropertyCriterion(VitalGraphQueryPropertyCriterion.URI).exists());
			}
			
		}
		
		
		nextPage();
		
		
	}

	private void nextPage() {

		
		resultsIndex = 0;

		resultsPage = new ArrayList<BindingEl>();
		
		if(connectorQuery != null) {
			
			nextConnectorsPage();
			
		} else {
			
			nextRootPage();
			
		}
		
	
		
		
	}

	private void nextRootPage() {

		if(noMorePages) return;
		
//		while(true) {
			
			ResultList rootPage = null;
			try {
//				rootPage = new SQLSelectQueryHandler(dataSource, connection, rootQuery, segments).execute();
				long start = System.currentTimeMillis();
				rootPage = new SQLSingleSegmentQueryHandler(dataSource, connection, rootQuery, segments, resolver, queryStats).execute();
				if(log.isDebugEnabled()) {
					log.debug("Root query: {}, time: {}ms", ""/*rootQuery.debugString()*/, System.currentTimeMillis() - start);
				}
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
			
			rootQuery.setOffset(rootQuery.getOffset() + PAGE_SIZE);
			
			if(rootPage.getStatus().getStatus() != VitalStatus.Status.ok) throw new RuntimeException("Error when querying for root endpoints: " + rootPage.getStatus());
			
			if(rootPage.getResults().size() < 1 ) {

				noMorePages = true;
				
//				break;
				
				//iterate over edges results and set new endpoints
				
			}
			
			int thisPageSize = 0;
			for(GraphObject g : rootPage) {
				
				resultsPage.add(new BindingEl(arc, g, null));
				thisPageSize ++;
				
			}
			
			if(thisPageSize < PAGE_SIZE) {
				noMorePages = true;
//				break;
				
			}
			
//		}
		
	}

	private void nextConnectorsPage() {

		if(noMorePages) return;
		
//		while(true) {

			ResultList connectorsPage = null;
			
			try {
//				connectorsPage = new SQLSelectQueryHandler(dataSource, connection, connectorQuery, segments).execute();
				long start = System.currentTimeMillis();
				connectorsPage = new SQLSingleSegmentQueryHandler(dataSource, connection, connectorQuery, segments, resolver, queryStats).execute();
				if(log.isDebugEnabled()) {
					log.debug("Connector query: {}, time: {}ms", ""/*connectorQuery.debugString()*/, System.currentTimeMillis() - start);
				}
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
			
//			ResultList connectorsPage = SelectQueryImpl.selectQuery(segments, connectorQuery);
			
			connectorQuery.setOffset(connectorQuery.getOffset() + PAGE_SIZE);
			
			if(connectorsPage.getStatus().getStatus() != VitalStatus.Status.ok) throw new RuntimeException("Error when querying for edge: " + connectorsPage.getStatus());
			
			if(connectorsPage.getResults().size() < 1 ) {

				noMorePages = true;
				return;
//				break;
				
				//iterate over edges results and set new endpoints
				
			}
			
			Map<String, GraphObject> endpoints = new HashMap<String, GraphObject>();
			
			for(GraphObject g : connectorsPage) {

				String endpointURI = getConnectorEndpointURI(g);
			
				endpoints.put(endpointURI, null);
				
			}

			if(endpoints.size() == 0) throw new RuntimeException("endpoints set must not be empty at this point");
			
			VitalSelectQuery equery = VitalSelectQuery.createInstance();
			equery.setOffset(0);
			equery.setLimit(PAGE_SIZE);
					
			VitalGraphCriteriaContainer endpointCriteriaContainer = equery.getCriteriaContainer();
			endpointCriteriaContainer.add(new VitalGraphQueryPropertyCriterion(VitalGraphQueryPropertyCriterion.URI).oneOf(toURIPropertiesList(endpoints.keySet())));
			
			if(arc.endpointContainer.endpointCriteria > 0) {
				endpointCriteriaContainer.add(arc.endpointContainer.container);
			}

			
			ResultList endpointsRS = null;
			
			try {
//				endpointsRS = new SQLSelectQueryHandler(dataSource, connection, equery, segments).execute();
				long start = System.currentTimeMillis();
				endpointsRS = new SQLSingleSegmentQueryHandler(dataSource, connection, equery, segments, resolver, queryStats).execute();
				if(log.isDebugEnabled()) {
					log.debug("Endpoints query {}, time: {}ms", ""/*equery.debugString()*/, System.currentTimeMillis() - start);
				}
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
			
			
//			ResultList endpointsRS = SelectQueryImpl.selectQuery(segments, equery);
			if(endpointsRS.getStatus().getStatus() != VitalStatus.Status.ok) throw new RuntimeException("Error when querying for edge: " + endpointsRS.getStatus());
			
			for(GraphObject g : endpointsRS) {
				
				endpoints.put(g.getURI(), g);
				
			}
			
			int edgesCount = 0;
			for(GraphObject connector : connectorsPage) {
				
				edgesCount++;
				
				String endpointURI = getConnectorEndpointURI(connector);
				
				GraphObject endpoint = endpoints.get(endpointURI);
				
				if(endpoint != null) {
					
					BindingEl b = new BindingEl(arc, endpoint, connector);
					
					resultsPage.add(b);
				}
				
			}
			
			if(edgesCount < PAGE_SIZE) {
				noMorePages = true;
			}
			
//			if(resultsPage.size() > 0) {
//				break;
//			} else {
//			}
			
//		}
		
	}

	private String getConnectorEndpointURI(GraphObject g) {
		
		String endpointURI = null;
		
		if(g instanceof VITAL_Edge) {
			
			VITAL_Edge e = (VITAL_Edge) g;
			
			if(arc.isForwardNotReverse()) {
				endpointURI = e.getDestinationURI();
			} else {
				endpointURI = e.getSourceURI();
			}
			
		} else if(g instanceof VITAL_HyperEdge) {
			
			VITAL_HyperEdge he = (VITAL_HyperEdge) g;
			
			if(arc.isForwardNotReverse()) {
				endpointURI = he.getDestinationURI();
			} else {
				endpointURI = he.getSourceURI();
			}
			
		} else throw new RuntimeException("Unexpected graph object result in connectors query: " + g.getClass().getCanonicalName());
		
		if(endpointURI == null) throw new RuntimeException("No endpoint URI found in : " + g);
		
		return endpointURI;

	}

	@Override
	public boolean hasNext() {
		return resultsIndex < resultsPage.size();
	}

	@Override
	public BindingEl next() {
		
		if(!hasNext()) throw new NoSuchElementException("No more results!");
		
		BindingEl el = resultsPage.get(resultsIndex);
		
		resultsIndex++;
		
		if(resultsIndex >= resultsPage.size()) {
			
				//get next page
				
			nextPage();
				
				//end of results!
		}
		
		return el;
		
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException(SqlBindingElementIterator.class.getSimpleName() + " does not support removals");
	}

	private List<URIProperty> toURIPropertiesList(Collection<String> nodesURIs) {
		List<URIProperty> l = new ArrayList<URIProperty>();
		for(String n : nodesURIs) {
			l.add(URIProperty.withString(n));
		}
		return l;
	}

}
