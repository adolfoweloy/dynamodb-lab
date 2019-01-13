package com.aeloy.dynamodblab.music;

import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.*;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@RestController
@RequestMapping("/music")
public class MusicController {

    private static final String TABLE = "Music";

    @PostMapping
    public void createInitialData() {
        AmazonDynamoDB amazonDynamoDB = AmazonDynamoDBClientBuilder.standard()
                .withCredentials(new EnvironmentVariableCredentialsProvider())
                .build();

        try {
            List<PutItemResult> results = createListOfMusics().stream()
                    .map(this::createItem)
                    .map(item -> amazonDynamoDB.putItem(TABLE, item))
                    .collect(Collectors.toList());

            System.out.println(results);
        } catch (Exception e) {
            System.out.println("Unable to add ");
        }
    }

    private Map<String, AttributeValue> createItem(Music music) {
        var item = new HashMap<String, AttributeValue>();
        item.put("Artist", new AttributeValue().withS(music.getArtist()));
        item.put("Song", new AttributeValue().withS(music.getSong()));
        IntStream.range(1, 10).forEach(i -> item.put("Field"+i, new AttributeValue().withS("Content " + i)));
        return item;
    }

    @GetMapping("/all")
    public List<Music> getAll() {
        AmazonDynamoDB amazonDynamoDB = AmazonDynamoDBClientBuilder.standard()
                .withCredentials(new EnvironmentVariableCredentialsProvider())
                .build();

        ScanRequest scanRequest = new ScanRequest().withTableName(TABLE);
        ScanResult result = amazonDynamoDB.scan(scanRequest);

        return result.getItems().stream()
                .map(this::createMusic)
                .collect(Collectors.toList());
    }

    private Music createMusic(Map<String, AttributeValue> item) {
        Music music = new Music();
        music.setArtist(item.get("Artist").getS());
        music.setSong(item.get("Song").getS());
        return music;
    }

    @GetMapping
    public Music get(
            @RequestParam String artist,
            @RequestParam(required = false) String song) {

        var amazonDynamoDB = AmazonDynamoDBClientBuilder.standard()
                .withCredentials(new EnvironmentVariableCredentialsProvider())
                .build();

        var request = new GetItemRequest();
        request
            .withTableName(TABLE)
            .addKeyEntry("Artist", new AttributeValue().withS(artist))
            .addKeyEntry("Song", new AttributeValue().withS(song));

        var item = amazonDynamoDB.getItem(request);

        Music music = new Music();
        music.setArtist(item.getItem().get("Artist").getS());
        music.setSong(item.getItem().get("Song").getS());

        return music;
    }

    public List<Music> createListOfMusics() {
        var list = new ArrayList<>();

        for (int i = 0; i < 1; i++) {
            list.add(new Music("Hoodoo Gurus", "1000 Miles Away" + i));
            list.add(new Music("Hoodoo Gurus", "Come anytime" + i));
            list.add(new Music("Hoodoo Gurus", "Night Must Fall" + i));
            list.add(new Music("Hoodoo Gurus", "Out That Door" + i));
            list.add(new Music("Man or Astroman", "9 Volt" + i));
            list.add(new Music("Man or Astroman", "A Simple Text File" + i));
            list.add(new Music("Man or Astroman", "Configuration 9" + i));
            list.add(new Music("Man or Astroman", "Defcon 5" + i));
            list.add(new Music("Man or Astroman", "Electrostatic Brainfield" + i));
            list.add(new Music("Man or Astroman", "Um Espectro sem Escala" + i));
        }

        return list;
    }
}
