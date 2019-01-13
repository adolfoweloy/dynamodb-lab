package com.aeloy.dynamodblab.note;

import com.aeloy.dynamodblab.http.ErrorMessage;
import io.atlassian.fugue.extensions.step.Steps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import static java.util.function.Function.identity;

@RestController
@RequestMapping("/notes")
public class NotesController {
    private static final Logger LOGGER = LoggerFactory.getLogger(NotesController.class);
    private final Notes notes;

    NotesController(Notes notes) {
        this.notes = notes;
    }

    @PostMapping
    public ResponseEntity<?> create(@RequestBody Note note) {
        return Steps.begin(notes.add(note))
                .yield(identity())
                .fold(this::error, ResponseEntity::ok);
    }

    @PutMapping
    public ResponseEntity<?> update(@RequestBody Note note) {
        return Steps.begin(notes.update(note))
                .yield(identity())
                .fold(this::error, ResponseEntity::ok);
    }

    @ResponseBody
    @GetMapping("/{user_id}")
    public ResponseEntity<?> getNotes(@PathVariable("user_id") String userId) {
        return Steps.begin(notes.getNotesWithKey(userId))
                .yield(identity())
                .fold(this::error, ResponseEntity::ok);
    }

    @ResponseBody
    @DeleteMapping("/{user_id}")
    public ResponseEntity<?> deleteByUserId(@RequestBody Note note) {
        return Steps.begin(notes.deleteByKey(note.getUserId(), note.getTimestamp()))
                .yield(identity())
                .fold(this::error, ResponseEntity::ok);
    }

    private ResponseEntity<?> error(ErrorMessage error) {
        LOGGER.info("Error: {}", error.getMessage());

        return ResponseEntity
                .status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(error.getMessage());
    }

}
