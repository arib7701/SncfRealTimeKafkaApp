package ribot.amandine.kafka.app.model;

import ribot.amandine.kafka.app.Disruption;

import java.util.List;

public class DisruptionApiResponse {

    private List<Disruption> disruptionList;

    public DisruptionApiResponse(List<Disruption> disruptionList) {
        this.disruptionList = disruptionList;
    }

    public List<Disruption> getDisruptionList() {
        return disruptionList;
    }

    public void setDisruptionList(List<Disruption> disruptionList) {
        this.disruptionList = disruptionList;
    }
}
