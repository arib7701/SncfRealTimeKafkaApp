package ribot.amandine.kafka.app.model;

import ribot.amandine.kafka.app.Disruption;

import java.util.List;

public class DisruptionApiResponse {

    private Integer itemsPerPage;
    private Integer totalResult;
    private Integer startPage;
    private List<Disruption> disruptionList;

    public DisruptionApiResponse(Integer itemsPerPage, Integer totalResult, Integer startPage, List<Disruption> disruptionList) {
        this.itemsPerPage = itemsPerPage;
        this.totalResult = totalResult;
        this.startPage = startPage;
        this.disruptionList = disruptionList;
    }

    public Integer getItemsPerPage() {
        return itemsPerPage;
    }

    public Integer getTotalResult() {
        return totalResult;
    }

    public Integer getStartPage() {
        return startPage;
    }

    public List<Disruption> getDisruptionList() {
        return disruptionList;
    }

    public void setDisruptionList(List<Disruption> disruptionList) {
        this.disruptionList = disruptionList;
    }
}
