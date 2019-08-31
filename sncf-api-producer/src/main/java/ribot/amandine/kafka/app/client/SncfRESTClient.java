package ribot.amandine.kafka.app.client;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.http.HttpException;
import org.joda.time.DateTime;
import org.json.JSONArray;
import org.json.JSONObject;
import ribot.amandine.kafka.app.Disruption;
import ribot.amandine.kafka.app.Information;
import ribot.amandine.kafka.app.Stop;
import ribot.amandine.kafka.app.Train;
import ribot.amandine.kafka.app.configuration.AppConfig;
import ribot.amandine.kafka.app.model.DisruptionApiResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class SncfRESTClient {

    private Integer itemsPerPage;
    private Integer currentPage = 2;
    private final Integer pageSize;
    private final String username;
    private final String password;

    public SncfRESTClient(AppConfig appConfig) {
        this.pageSize = appConfig.getPageSize();
        this.username = appConfig.getUsername();
        this.password = appConfig.getPassword();
    }

    private void init() throws HttpException {
        itemsPerPage = disruptionApi(1, 1).getItemsPerPage();
        // we fetch from the last page
        currentPage = 2;
    }

    public List<Disruption> getNextDisruptions() throws HttpException {
        if (currentPage == null) init();
        if (currentPage >= 0) {
            List<Disruption> result = disruptionApi(pageSize, currentPage).getDisruptionList();
            currentPage -= 1;

            return result;
        }

        return Collections.emptyList();
    }

    public DisruptionApiResponse disruptionApi(Integer pageSize, Integer page) throws HttpException {
        String url = "https://api.sncf.com/v1/coverage/sncf/disruptions?start_page="+currentPage;
        HttpResponse<JsonNode> jsonResponse = null;
        try {
            jsonResponse = Unirest.get(url).basicAuth(username, password)
                    .asJson();
        } catch (UnirestException e) {
            throw new HttpException(e.getMessage());
        }

       if (jsonResponse.getStatus() == 200) {
            JSONObject body = jsonResponse.getBody().getObject();

            JSONObject pagination = body.getJSONObject("pagination");
            Integer itemsPerPage = pagination.getInt("items_per_page");

            Integer totalResult = pagination.getInt("total_result");
            Integer startPage = pagination.getInt("start_page");

            List<Disruption> disruptions = this.convertResults(body.getJSONArray("disruptions"));
            DisruptionApiResponse disruptionApiResponse = new DisruptionApiResponse(itemsPerPage, totalResult, startPage, disruptions);

            return disruptionApiResponse;
        }

        throw new HttpException("Sncf API Unavailable");
    }

    public List<Disruption> convertResults(JSONArray resultsJsonArray) {
        List<Disruption> results = new ArrayList<>();
        for (int i = 0; i < resultsJsonArray.length(); i++) {
            JSONObject disruptionJson = resultsJsonArray.getJSONObject(i);
            Disruption disruption = jsonToDisruption(disruptionJson);
            results.add(disruption);
        }
        //results.sort(Comparator.comparing(Disruption::getCreated));
        return results;
    }

    public Disruption jsonToDisruption(JSONObject disruptionJson) {
        Disruption.Builder disruptionBuilder = Disruption.newBuilder();
        disruptionBuilder.setId(disruptionJson.getString("id"));
        disruptionBuilder.setStatus(disruptionJson.getString("status"));
        disruptionBuilder.setMessage(disruptionJson.getString("message"));
        disruptionBuilder.setName(disruptionJson.getString("name"));
        disruptionBuilder.setCause(disruptionJson.getString("cause"));
        disruptionBuilder.setPriority(disruptionJson.getString("priority"));
        disruptionBuilder.setTrain(jsonToTrain(disruptionJson.getJSONObject("train")));
        disruptionBuilder.setStops(jsonToListStop(disruptionJson.getJSONObject("stop")));
        return disruptionBuilder.build();
    }

    public Train jsonToTrain(JSONObject trainJson) {
        Train.Builder trainBuilder = Train.newBuilder();
        trainBuilder.setId(trainJson.getString("id"));
        trainBuilder.setName(trainJson.getString("name"));
        trainBuilder.setType(trainJson.getString("type"));
        trainBuilder.setDisplayName(trainJson.getString("display_name"));

        return trainBuilder.build();
    }

    public List<Stop> jsonToListStop(JSONObject stops) {

        List<Stop> stopsList = new ArrayList<>();

        JSONArray stopsArray = stops.getJSONArray("impacted_stops");

        for (int i = 0; i < stopsArray.length(); i++) {

        }

        return stopsList;
    }


    public Stop jsonToStops(JSONObject stopJson) {
        Stop.Builder stopBuilder = Stop.newBuilder();
        stopBuilder.setId(stopJson.getString("id"));
        stopBuilder.setName(stopJson.getString("name"));
        stopBuilder.setLatitude(stopJson.getString("lat"));
        stopBuilder.setLongitude(stopJson.getString("long"));
        stopBuilder.setTimes(jsonToListInformation(stopJson.getJSONObject("times")));

        return stopBuilder.build();
    }

    public List<Information> jsonToListInformation(JSONObject listInformation) {

        List<Information> informations = new ArrayList<>();



        return informations;
    }

    public Information jsonToInformation(JSONObject stopJson) {
        Information.Builder stopBuilder = Information.newBuilder();
        stopBuilder.setBaseDepartureTime(stopJson.getString("base_departure_time"));
        stopBuilder.setBaseArrivalTime(stopJson.getString("base_arrival_time"));
        stopBuilder.setDepartureStatus(stopJson.getString("departure_status"));
        stopBuilder.setArrivalStatus(stopJson.getString("arrival_statud"));
        stopBuilder.setCause(stopJson.getString("cause"));
        stopBuilder.setIsDetour(stopJson.getBoolean("is_detour"));
        stopBuilder.setNewDepartureTime(stopJson.getString("new_departure_time"));
        stopBuilder.setNewArrivalTime(stopJson.getString("new_arrival_time"));

        return stopBuilder.build();
    }

    public void close() {
        try {
            Unirest.shutdown();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}


