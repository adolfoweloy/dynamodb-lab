package com.aeloy.dynamodblab.music;

import lombok.Data;

@Data
public class Music {
    private String artist;
    private String song;

    public Music() {}

    public Music(String artist, String song) {
        this.artist = artist;
        this.song = song;
    }
}
