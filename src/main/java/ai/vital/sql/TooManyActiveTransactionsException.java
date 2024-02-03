package ai.vital.sql;

/**
 * thrown when the number of active transactions exceeds some limit
 */
public class TooManyActiveTransactionsException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	private int limit;

	public TooManyActiveTransactionsException(String message, int limit) {
		super(message);
		this.limit = limit;
	}

	public int getLimit() {
		return limit;
	}

	public void setLimit(int limit) {
		this.limit = limit;
	}
	
	
	
}
