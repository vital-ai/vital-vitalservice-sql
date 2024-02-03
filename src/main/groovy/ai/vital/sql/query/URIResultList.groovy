package ai.vital.sql.query

import java.util.List

/**
 * internal results container
 *
 */
class URIResultList {

	public List<URIResultElement> results
	
	public Integer totalResults
	
	public Integer offset
	
	public Integer limit

	//aggregation only!
	public Double agg_sum;
	
	public Integer agg_count;
	
	public Integer agg_count_distinct;
	
	public Double agg_min;
	
	public Double agg_max;
	
	public List distinctValues = null


	public Double getAgg_sum() {
		return agg_sum;
	}

	public void setAgg_sum(Double agg_sum) {
		this.agg_sum = agg_sum;
	}

	public Integer getAgg_count() {
		return agg_count;
	}

	public void setAgg_count(Integer agg_count) {
		this.agg_count = agg_count;
	}

	public Integer getAgg_count_distinct() {
		return agg_count_distinct;
	}

	public void setAgg_count_distinct(Integer agg_count_distinct) {
		this.agg_count_distinct = agg_count_distinct;
	}

	public Double getAgg_min() {
		return agg_min;
	}

	public void setAgg_min(Double agg_min) {
		this.agg_min = agg_min;
	}

	public Double getAgg_max() {
		return agg_max;
	}

	public void setAgg_max(Double agg_max) {
		this.agg_max = agg_max;
	}

	public List getDistinctValues() {
		return distinctValues;
	}

	public void setDistinctValues(List distinctValues) {
		this.distinctValues = distinctValues;
	}

	public List<URIResultElement> getResults() {
		return results;
	}

	public void setResults(List<URIResultElement> results) {
		this.results = results;
	}

	public Integer getTotalResults() {
		return totalResults;
	}

	public void setTotalResults(Integer totalResults) {
		this.totalResults = totalResults;
	}

	public Integer getOffset() {
		return offset;
	}

	public void setOffset(Integer offset) {
		this.offset = offset;
	}

	public Integer getLimit() {
		return limit;
	}

	public void setLimit(Integer limit) {
		this.limit = limit;
	}

}
