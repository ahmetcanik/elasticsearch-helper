package elasticsearch.helper;

public class ElasticEntity {
    private String _id;

    public ElasticEntity() {
    }

    public ElasticEntity(String _id) {
        this._id = _id;
    }

    public String get_id() {
        return _id;
    }

    public void set_id(String _id) {
        this._id = _id;
    }
}
