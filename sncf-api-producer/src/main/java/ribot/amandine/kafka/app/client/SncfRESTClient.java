package ribot.amandine.kafka.app.client;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.http.HttpException;
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
import java.util.List;

public class SncfRESTClient {

    private Integer currentPage;
    private final String username;
    private final String password;

    public SncfRESTClient(AppConfig appConfig) {
        this.username = appConfig.getUsername();
        this.password = appConfig.getPassword();
    }

    public List<Disruption> getNextDisruptions(Integer startPage) throws HttpException {

        currentPage = startPage;

        if (currentPage >= 0) {
            return disruptionApi().getDisruptionList();
        }

        return Collections.emptyList();
    }

    private DisruptionApiResponse disruptionApi() throws HttpException {
        String url = "https://api.sncf.com/v1/coverage/sncf/disruptions?start_page=" + currentPage;
        HttpResponse<JsonNode> jsonResponse;
        try {
            jsonResponse = Unirest.get(url).basicAuth(username, password).asJson();
        } catch (UnirestException e) {
            throw new HttpException(e.getMessage());
        }

        if (jsonResponse.getStatus() == 200) {
            JSONObject body = jsonResponse.getBody().getObject();

            JSONObject pagination = body.getJSONObject("pagination");

            Integer totalResult = pagination.getInt("total_result");
            Integer startPage = pagination.getInt("start_page");

            List<Disruption> disruptions = this.convertResults(body.getJSONArray("disruptions"));

            return new DisruptionApiResponse(totalResult, startPage, disruptions);
        }

        throw new HttpException("Sncf API Unavailable");
    }

    private List<Disruption> convertResults(JSONArray resultsJsonArray) {

        List<Disruption> results = new ArrayList<>();
        for (int i = 0; i < resultsJsonArray.length(); i++) {
            JSONObject disruptionJson = resultsJsonArray.getJSONObject(i);
            Disruption disruption = jsonToDisruption(disruptionJson);
            results.add(disruption);
        }
        return results;
    }

    private Disruption jsonToDisruption(JSONObject disruptionJson) {

        JSONObject severityJson = disruptionJson.getJSONObject("severity");
        JSONArray messagesArrayJson;
        JSONObject firstMessageJson;
        String textMessage = null;

        try {
            messagesArrayJson = disruptionJson.getJSONArray("messages");
            firstMessageJson = messagesArrayJson.getJSONObject(0);
            textMessage = firstMessageJson.getString("text");
        } catch (Exception e) {
            //System.out.println(e);
        }

        JSONArray impactedObjectsArrayJson = disruptionJson.getJSONArray("impacted_objects");
        JSONObject firstImpactedObjectsJson = impactedObjectsArrayJson.getJSONObject(0);

        return Disruption.newBuilder()
                .setId(disruptionJson.getString("id"))
                .setStatus(disruptionJson.getString("status"))
                .setUpdatedAt(disruptionJson.getString("updated_at"))
                .setCause(disruptionJson.getString("cause"))
                .setPriority(Integer.toString(severityJson.getInt("priority")))
                .setName(severityJson.getString("name"))
                .setMessage(textMessage)
                .setTrain(jsonToTrain(firstImpactedObjectsJson.getJSONObject("pt_object")))
                .setStops(jsonToListStop(firstImpactedObjectsJson.getJSONArray("impacted_stops")))
                .build();
    }

    private Train jsonToTrain(JSONObject trainJson) {

        JSONObject tripJson = trainJson.getJSONObject("trip");

        return Train.newBuilder()
                .setId(tripJson.getString("id"))
                .setName(tripJson.getString("name"))
                .setType("typeUnknown")
                .setDisplayName(tripJson.getString("name"))
                .build();

    }

    private List<Stop> jsonToListStop(JSONArray stopsArray) {

        List<Stop> stopsList = new ArrayList<>();

        for (int i = 0; i < stopsArray.length(); i++) {
            Stop stop = jsonToStops(stopsArray.getJSONObject(i));
            stopsList.add(stop);
        }

        return stopsList;
    }


    private Stop jsonToStops(JSONObject stopJson) {

        JSONObject stopPointJson = stopJson.getJSONObject("stop_point");
        JSONObject coordJson = stopPointJson.getJSONObject("coord");

        return Stop.newBuilder()
                .setId(stopPointJson.getString("id"))
                .setName(stopPointJson.getString("name"))
                .setLatitude(coordJson.getString("lat"))
                .setLongitude(coordJson.getString("lon"))
                .setTimes(jsonToInformation(stopJson))
                .build();

    }

    private Information jsonToInformation(JSONObject stopJson) {

        String baseDepartureTime = null;
        String baseArrivalTime = null;
        String amendedDepartureTime = null;
        String amendedArrivalTime = null;

        try {
            baseDepartureTime = stopJson.getString("base_departure_time");
            baseArrivalTime = stopJson.getString("base_arrival_time");
            amendedDepartureTime = stopJson.getString("amended_departure_time");
            amendedArrivalTime = stopJson.getString("amended_arrival_time");
        } catch (Exception e) {
           //System.out.println(e);
        }

        return Information.newBuilder()
                .setBaseDepartureTime(baseDepartureTime)
                .setBaseArrivalTime(baseArrivalTime)
                .setDepartureStatus(stopJson.getString("departure_status"))
                .setArrivalStatus(stopJson.getString("arrival_status"))
                .setCause(stopJson.getString("cause"))
                .setIsDetour(stopJson.getBoolean("is_detour"))
                .setNewDepartureTime(amendedDepartureTime)
                .setNewArrivalTime(amendedArrivalTime)
                .build();
    }

    public void close() {
        try {
            Unirest.shutdown();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}


