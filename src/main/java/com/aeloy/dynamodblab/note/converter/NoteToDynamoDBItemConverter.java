package com.aeloy.dynamodblab.note.converter;

import com.aeloy.dynamodblab.note.Note;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import org.springframework.core.convert.converter.Converter;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
public class NoteToDynamoDBItemConverter implements Converter<Note, Map<String, AttributeValue>> {
    @Override
    public Map<String, AttributeValue> convert(Note note) {
        var item = new HashMap<String, AttributeValue>();

        item.put("user_id", new AttributeValue().withS(note.getUserId()));
        item.put("timestamp", new AttributeValue().withS(Long.toString(note.getTimestamp())));
        item.put("cat", new AttributeValue().withS(note.getCategory()));
        item.put("title", new AttributeValue().withS(note.getTitle()));
        item.put("note_id", new AttributeValue().withS(note.getNoteId()));
        item.put("user_name", new AttributeValue().withS(note.getUserName()));
        item.put("content", new AttributeValue().withS(note.getContent()));
        return item;
    }
}
