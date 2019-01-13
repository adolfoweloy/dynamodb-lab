package com.aeloy.dynamodblab.note.converter;

import com.aeloy.dynamodblab.note.Note;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import io.atlassian.fugue.Option;
import org.springframework.core.convert.converter.Converter;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class DynamoDBItemToNoteConverter implements Converter<Map<String, AttributeValue>, Note> {

    @Override
    public Note convert(Map<String, AttributeValue> item) {
        Note note = new Note();

        note.setUserId(getStringValueFrom(item, "user_id"));
        note.setTimestamp(Long.parseLong(getStringValueFrom(item, "timestamp")));

        note.setCategory(getStringValueFrom(item, "cat"));
        note.setContent(getStringValueFrom(item, "content"));
        note.setNoteId(getStringValueFrom(item, "note_id"));
        note.setTitle(getStringValueFrom(item, "title"));
        note.setUserName(getStringValueFrom(item, "user_name"));

        return note;
    }

    private String getStringValueFrom(Map<String, AttributeValue> item, String attributeName) {
        return Option.option(item.get(attributeName))
            .fold(() -> null, AttributeValue::getS);
    }
}
