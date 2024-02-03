package ai.vital.sql.query

import java.util.Map;
import ai.vital.sql.model.SegmentTable;

class URIResultElement {

	public String URI

	//keep the reference to segment object to avoid miss-lookups
	public SegmentTable segment
	
	public double score
	
	//raw values ?
	public Map<String, Object> attributes
	
	@Override
	public boolean equals(Object obj) {
		return (obj instanceof URIResultElement) && ((URIResultElement)obj).URI.equals(URI);
	}	
}