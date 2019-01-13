package com.aeloy.dynamodblab.note;

import com.aeloy.dynamodblab.http.ErrorMessage;
import com.aeloy.dynamodblab.note.converter.DynamoDBItemToNoteConverter;
import com.aeloy.dynamodblab.note.converter.NoteToDynamoDBItemConverter;
import com.aeloy.dynamodblab.note.definitions.NoteFields;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.*;
import io.atlassian.fugue.Either;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.atlassian.fugue.Either.left;
import static io.atlassian.fugue.Either.right;

@Component
public class Notes {
    private static final Logger LOGGER = LoggerFactory.getLogger(Notes.class);
    private static final String TABLE = "tb_notes";
    private final AmazonDynamoDB amazonDynamoDB;
    private final DynamoDB dynamoDB;
    private final DynamoDBItemToNoteConverter dynamoDBItemToNoteConverter;
    private final NoteToDynamoDBItemConverter noteToDynamoDBItemConverter;

    public Notes(AmazonDynamoDB amazonDynamoDB,
                 DynamoDB dynamoDB, DynamoDBItemToNoteConverter dynamoDBItemToNoteConverter,
                 NoteToDynamoDBItemConverter noteToDynamoDBItemConverter) {
        this.amazonDynamoDB = amazonDynamoDB;
        this.dynamoDB = dynamoDB;
        this.dynamoDBItemToNoteConverter = dynamoDBItemToNoteConverter;
        this.noteToDynamoDBItemConverter = noteToDynamoDBItemConverter;
    }

    /**
     * This method is an example of a PutItem operation that
     * uses ConditionExpression to validate against entry updates.
     *
     * @param note A note to be added to the database
     * @return Returns the Note added or an Error
     */
    Either<ErrorMessage, Note> add(Note note) {
        PutItemRequest request = new PutItemRequest()
                .withTableName(TABLE)
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                .withItem(noteToDynamoDBItemConverter.convert(note))
                .withConditionExpression("#timestamp <> :timestamp")
                .addExpressionAttributeNamesEntry("#timestamp", "timestamp")
                .addExpressionAttributeValuesEntry(":timestamp",
                        new AttributeValue().withS(Long.toString(note.getTimestamp())));

        try {
            PutItemResult result = amazonDynamoDB.putItem(request);

            LOGGER.info(String.format("consumed WCUs: %f", result.getConsumedCapacity().getWriteCapacityUnits()));
            LOGGER.info(String.format("consumed RCUs: %f", result.getConsumedCapacity().getReadCapacityUnits()));

            return right(note);
        } catch (ConditionalCheckFailedException e) {
            return left(new ErrorMessage("cannot change item while trying to add an entry"));
        }
    }

    /**
     * An example of PutItem operation that allows for updates to be performed.
     *
     * @param note Note to be updated
     * @return returns the old state of a note being updated or an error message
     */
    Either<ErrorMessage, Note> update(Note note) {
        UpdateItemRequest request = new UpdateItemRequest()
                .withTableName(TABLE)
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                .withReturnValues(ReturnValue.ALL_OLD)
                .addKeyEntry(NoteFields.USER_ID, new AttributeValue().withS(note.getUserId()))
                .addKeyEntry(NoteFields.TIMESTAMP, new AttributeValue().withS(Long.toString(note.getTimestamp())))
                .withAttributeUpdates(getAttributeUpdates(note));

        try {
            UpdateItemResult result = amazonDynamoDB.updateItem(request);

            LOGGER.info(String.format("consumed WCUs: %f", result.getConsumedCapacity().getWriteCapacityUnits()));
            LOGGER.info(String.format("consumed RCUs: %f", result.getConsumedCapacity().getReadCapacityUnits()));

            return right(note);
        } catch (AmazonDynamoDBException e) {
            return left(new ErrorMessage("it was not possible to update the requested entry"));
        }
    }

    private Map<String,AttributeValueUpdate> getAttributeUpdates(Note note) {
        var attributes = new HashMap<String, AttributeValueUpdate>();
        attributes.put(NoteFields.CONTENT,
                new AttributeValueUpdate().withValue(new AttributeValue().withS(note.getContent())));
        attributes.put(NoteFields.CATEGORY,
                new AttributeValueUpdate().withValue(new AttributeValue().withS(note.getCategory())));
        attributes.put(NoteFields.TITLE,
                new AttributeValueUpdate().withValue(new AttributeValue().withS(note.getTitle())));
        return attributes;
    }

    /**
     * Uses document interface to run data plane operations against DynamoDB
     * @param userId user identifier
     * @return a list of notes in case of success or error message otherwise
     */
    Either<ErrorMessage, List<Note>> getNotesWithKey(String userId) {
        try {
            var notes = new ArrayList<Note>();

            DynamoDB client = new DynamoDB(amazonDynamoDB);
            QuerySpec spec = new QuerySpec()
                    .withConsistentRead(true) // just to see the consumed RCUs = 1.0
                    .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                    .withKeyConditionExpression("#user_id = :user_id")
                        .withNameMap(new NameMap().with("#user_id", NoteFields.USER_ID))
                        .withValueMap(new ValueMap().withString(":user_id", userId));

            ItemCollection<QueryOutcome> result = client.getTable(TABLE).query(spec);
            result.forEach(item -> notes.add(convertItemToNote(item)));

            LOGGER.info(String.format("consumed RCUs: %f",
                    result.getAccumulatedConsumedCapacity().getCapacityUnits()));

            return right(notes);
        } catch (AmazonDynamoDBException e) {
            return left(new ErrorMessage(e.getMessage()));
        }
    }

    private Note convertItemToNote(Item item) {
        Note note = new Note();
        note.setUserName(item.getString(NoteFields.USER_NAME));
        note.setTitle(item.getString(NoteFields.TITLE));
        note.setUserId(item.getString(NoteFields.USER_ID));
        note.setTimestamp(Long.parseLong(item.getString(NoteFields.TIMESTAMP)));
        note.setCategory(item.getString(NoteFields.CATEGORY));
        note.setNoteId(item.getString(NoteFields.NOTE_ID));
        note.setContent(item.getString(NoteFields.CONTENT));
        return note;
    }

    Either<ErrorMessage, Note> deleteByKey(String userId, Long timestamp) {
        DeleteItemRequest request = new DeleteItemRequest()
                .withTableName(TABLE)
                .addKeyEntry(NoteFields.USER_ID, new AttributeValue().withS(userId))
                .addKeyEntry(NoteFields.TIMESTAMP, new AttributeValue().withS(Long.toString(timestamp)))
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                .withReturnValues(ReturnValue.ALL_OLD);

        try {
            DeleteItemResult result = amazonDynamoDB.deleteItem(request);

            LOGGER.info(String.format("consumed WCUs: %f", result.getConsumedCapacity().getWriteCapacityUnits()));
            LOGGER.info(String.format("consumed RCUs: %f", result.getConsumedCapacity().getReadCapacityUnits()));

            int httpStatusCode = result.getSdkHttpMetadata().getHttpStatusCode();
            if (httpStatusCode / 100 == 2 && result.getAttributes() != null) {
                return right(dynamoDBItemToNoteConverter.convert(result.getAttributes()));
            } else {
                LOGGER.error(String.format("Http error: %d", httpStatusCode));
                return left(new ErrorMessage("the item specified item cannot be deleted"));
            }

        } catch (AmazonDynamoDBException e) {
            LOGGER.error(e.getMessage(), e);
            return left(new ErrorMessage(String.format("items for %s cannot be deleted", userId)));
        }
    }
}
