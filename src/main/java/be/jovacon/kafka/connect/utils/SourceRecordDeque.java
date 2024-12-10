package be.jovacon.kafka.connect.utils;

import org.apache.kafka.connect.source.SourceRecord;

import java.util.Deque;
import java.util.List;

public interface SourceRecordDeque extends Deque<SourceRecord> {
    /**
     * Method returns a new list that has an initial capacity of the batch size the deque is configured
     * for.
     * @return New list with an initial capacity at the batch size.
     */
    List<SourceRecord> newList();

    /**
     * Method will create a new list based on the batch size and drain records to it.
     * @return
     */
    @Deprecated
    List<SourceRecord> drain();

    /**
     * Method will create a new list based on the batch size and drain records to it.
     * @return
     */
    List<SourceRecord> getBatch(int emptyWaitMs);
    /**
     * Method will create a new list based on the batch size and drain records to it.
     * @return
     */
    List<SourceRecord> getBatch();
    /**
     * Method is used drain the records from the queue to the supplied list. newList() should be called
     * to create a list that has the same initial capacity of the batch size.
     * @param records List to append the records to.
     * @return true if records were drained. false if not.
     *
     */
    @Deprecated
    boolean drain(List<SourceRecord> records);

    /**
     * Method is used drain the records from the queue to the supplied list. newList() should be called
     * to create a list that has the same initial capacity of the batch size.
     * @param records List to append the records to.
     * @param emptyWaitMs Time in milliseconds to wait if no records were drained.
     * @return true if records were drained. false if not.
     */
    @Deprecated
    boolean drain(List<SourceRecord> records, int emptyWaitMs);
}