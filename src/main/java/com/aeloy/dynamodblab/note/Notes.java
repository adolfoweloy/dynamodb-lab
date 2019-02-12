package com.aeloy.dynamodblab.note;

import com.aeloy.dynamodblab.http.ErrorMessage;
import com.aeloy.dynamodblab.note.definitions.NoteFields;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec;
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

    public Notes(AmazonDynamoDB amazonDynamoDB,
                 DynamoDB dynamoDB) {
        this.amazonDynamoDB = amazonDynamoDB;
        this.dynamoDB = dynamoDB;
    }

    /**
     * This method is an example of a PutItem operation that
     * uses ConditionExpression to validate against entry updates.
     *
     * @param note A note to be added to the database
     * @return Returns the Note added or an Error
     */
    Either<ErrorMessage, Note> add(Note note) {
        Item item = new Item()
                .withPrimaryKey(new PrimaryKey()
                        .addComponent(NoteFields.USER_ID, note.getUserId())
                        .addComponent(NoteFields.TIMESTAMP, Long.toString(note.getTimestamp())))
                .withString(NoteFields.CONTENT, note.getContent())
                .withString(NoteFields.NOTE_ID, note.getNoteId())
                .withString(NoteFields.CATEGORY, note.getCategory())
                .withString(NoteFields.TITLE, note.getTitle())
                .withString(NoteFields.USER_NAME, note.getUserName());

        PutItemSpec putSpec = new PutItemSpec()
                .withItem(item)
                .withReturnValues(ReturnValue.ALL_OLD)
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                .withConditionExpression("#t <> :t")
                    .withNameMap(new NameMap().with("#t", NoteFields.TIMESTAMP))
                    .withValueMap(new ValueMap().with(":t", Long.toString(note.getTimestamp())));

        try {
            PutItemOutcome result = dynamoDB.getTable(TABLE).putItem(putSpec);

            LOGGER.info(String.format("consumed RCUs: %f", result.getPutItemResult()
                    .getConsumedCapacity().getCapacityUnits()));

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
            var notes = new ArrayList<>();

            QuerySpec spec = new QuerySpec()
                    .withConsistentRead(true) // just to see the consumed RCUs = 1.0
                    .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                    .withKeyConditionExpression("#user_id = :user_id")
                        .withNameMap(new NameMap().with("#user_id", NoteFields.USER_ID))
                        .withValueMap(new ValueMap().withString(":user_id", userId));

            ItemCollection<QueryOutcome> result = dynamoDB.getTable(TABLE).query(spec);
            result.forEach(item -> notes.add(convertItemToNote(item)));

            LOGGER.info(String.format("consumed RCUs: %f",
                    result.getAccumulatedConsumedCapacity().getCapacityUnits()));

            return right(notes);
        } catch (AmazonDynamoDBException e) {
            return left(new ErrorMessage(e.getMessage()));
        }
    }

    /**
     * Delete operations using document interfaces.
     * @param userId        partition key
     * @param timestamp     sort key
     * @return returns the note deleted or error message otherwise.
     */
    Either<ErrorMessage, Note> deleteByKey(String userId, Long timestamp) {
        DeleteItemSpec spec = new DeleteItemSpec()
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                .withReturnValues(ReturnValue.ALL_OLD)
                .withPrimaryKey(new PrimaryKey()
                    .addComponent(NoteFields.USER_ID, userId)
                    .addComponent(NoteFields.TIMESTAMP, timestamp.toString()));

        try {
            DeleteItemOutcome outcome = dynamoDB.getTable(TABLE).deleteItem(spec);
            DeleteItemResult result = outcome.getDeleteItemResult();

            LOGGER.info(String.format("consumed capacity: %f", result.getConsumedCapacity().getCapacityUnits()));

            int httpStatusCode = result.getSdkHttpMetadata().getHttpStatusCode();
            if (httpStatusCode / 100 == 2 && result.getAttributes() != null) {
                return right(convertItemToNote(outcome.getItem()));
            } else {
                LOGGER.error(String.format("Http error: %d", httpStatusCode));
                return left(new ErrorMessage("the item specified item cannot be deleted"));
            }

        } catch (AmazonDynamoDBException e) {
            LOGGER.error(e.getMessage(), e);
            return left(new ErrorMessage(String.format("items for %s cannot be deleted", userId)));
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

}
