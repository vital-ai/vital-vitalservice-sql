package ai.vital.sql.query;

import java.sql.Connection;
import java.util.Iterator;
import java.util.List;

import ai.vital.sql.connector.VitalSqlDataSource;
import ai.vital.sql.model.SegmentTable;
import ai.vital.vitalservice.query.QueryStats;
import ai.vital.vitalsigns.model.GraphObject;
import ai.vital.vitalsigns.query.graph.Arc;
import ai.vital.vitalsigns.query.graph.BindingEl;
import ai.vital.vitalsigns.query.graph.GraphQueryImplementation.ResultsProvider;

public class SqlResultsProvider implements ResultsProvider {

	final List<SegmentTable> segments;
	private Connection connection;
	private VitalSqlDataSource dataSource;
	private SQLGraphObjectResolver resolver;
	private QueryStats queryStats;
	
	public SqlResultsProvider(VitalSqlDataSource dataSource, Connection connection, List<SegmentTable> segments, SQLGraphObjectResolver resolver, QueryStats queryStats) {
		super();
		this.dataSource = dataSource;
		this.connection = connection;
		this.segments = segments;
		this.resolver = resolver;
		this.queryStats = queryStats;
	}

	@Override
	public Iterator<BindingEl> getIterator(Arc arc, GraphObject parent) {
		return new SqlBindingElementIterator(dataSource, connection, segments, arc, parent, resolver, queryStats);
		
	}

}
