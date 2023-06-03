package ru.otus.services.processors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.otus.lib.SensorDataBufferedWriter;
import ru.otus.api.SensorDataProcessor;
import ru.otus.api.model.SensorData;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;



// Этот класс нужно реализовать

public class SensorDataProcessorBuffered implements SensorDataProcessor {
    private static final Logger log = LoggerFactory.getLogger(SensorDataProcessorBuffered.class);

    private int bufferSize;
    private SensorDataBufferedWriter writer;
    private ConcurrentSkipListSet<SensorData> dataBuffer;
    private Lock lock = new ReentrantLock();

    public SensorDataProcessorBuffered(int bufferSize, SensorDataBufferedWriter writer) {
        this.bufferSize = bufferSize;
        this.writer = writer;
        this.dataBuffer = new ConcurrentSkipListSet<>(Comparator.comparing(SensorData::getMeasurementTime));
    }

    @Override
    public void process(SensorData data) {
        lock.lock();
        try {
            dataBuffer.add(data);

            if (dataBuffer.size() >= bufferSize) {
                flush();
            }
        } finally {
            lock.unlock();
        }
    }

    public void flush() {
        lock.lock();
        try {
            while (!dataBuffer.isEmpty() && dataBuffer.size() >= bufferSize) {
                List<SensorData> dataToWrite = new ArrayList<>(bufferSize);
                for (int i = 0; i < bufferSize; i++) {
                    dataToWrite.add(dataBuffer.pollFirst());
                }
                writer.writeBufferedData(dataToWrite);
            }

            if (!dataBuffer.isEmpty()) {
                writer.writeBufferedData(new ArrayList<>(dataBuffer));
                dataBuffer.clear();
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void onProcessingEnd() {
        flush();
    }
}