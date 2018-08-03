package elastic.helper;

public class ElasticEntity {
	private String id;

	public ElasticEntity() {
	}

	public ElasticEntity(String id) {
		this.id = id;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}
}
