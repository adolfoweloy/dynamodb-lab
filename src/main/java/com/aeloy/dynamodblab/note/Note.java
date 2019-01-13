package com.aeloy.dynamodblab.note;

import lombok.Data;
import javax.validation.constraints.NotNull;

@Data
public class Note {
    @NotNull
    private String userId;
    @NotNull
    private long timestamp;
    private String category;
    private String title;
    private String content;
    private String noteId;
    private String userName;
}
