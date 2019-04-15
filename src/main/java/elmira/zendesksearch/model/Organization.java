package elmira.zendesksearch.model;

import java.util.List;

public class Organization {

    private String _id;
    private String url;
    private String external_id;
    private String name;
    private List<String> domain_names;
    private String created_at;
    private String details;
    private Boolean shared_tickets;
    private List<String> tags;


    public String get_id() {
        return _id;
    }

    public void set_id(String _id) {
        this._id = _id;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getExternal_id() {
        return external_id;
    }

    public void setExternal_id(String external_id) {
        this.external_id = external_id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<String> getDomain_names() {
        return domain_names;
    }

    public void setDomain_names(List<String> domain_names) {
        this.domain_names = domain_names;
    }

    public String getCreated_at() {
        return created_at;
    }

    public void setCreated_at(String created_at) {
        this.created_at = created_at;
    }

    public String getDetails() {
        return details;
    }

    public void setDetails(String details) {
        this.details = details;
    }

    public Boolean getShared_tickets() {
        return shared_tickets;
    }

    public void setShared_tickets(Boolean shared_tickets) {
        this.shared_tickets = shared_tickets;
    }

    public List<String> getTags() {
        return tags;
    }

    public void setTags(List<String> tags) {
        this.tags = tags;
    }
}
