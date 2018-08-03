package elasticsearch.helper;

public class SearchResult {
	private String result;
	private String snippet;
	private int from;
	private int size;

	public SearchResult() {
	}

	public SearchResult(String result, int from, int size) {
		this.result = result;
		this.from = from;
		this.size = size;
	}

	public String getResult() {
		return result;
	}

	public void setResult(String result) {
		this.result = result;
	}

	public String getSnippet() {
		return snippet;
	}

	public void setSnippet(String snippet) {
		this.snippet = snippet;
	}

	public int getFrom() {
		return from;
	}

	public void setFrom(int from) {
		this.from = from;
	}

	public int getSize() {
		return size;
	}

	public void setSize(int size) {
		this.size = size;
	}
}
