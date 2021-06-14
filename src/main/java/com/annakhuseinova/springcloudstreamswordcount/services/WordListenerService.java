package com.annakhuseinova.springcloudstreamswordcount.services;

import com.annakhuseinova.springcloudstreamswordcount.bindings.WordListenerBinding;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.stereotype.Service;

import static java.util.Arrays.asList;

@Service
@Slf4j
@EnableBinding(WordListenerBinding.class)
public class WordListenerService {

    @StreamListener("words-input-channel")
    public void process(KStream<String, String> input){
        KStream<String, String> wordStream = input.flatMapValues(value -> asList(value.toLowerCase().split(" ")));
        // Kafka Streams allows to group records only by key. The groupBy() method takes key, value and returns the
        // grouping key. The groupBy will do the grouping for us. If your stream comes with appropriate keys, you can
        // use groupByKey() method.
        wordStream.groupBy((key, value)-> value)
                .count()
                .toStream()
                .peek((key, value)-> log.info("Word: {} Count: {}", key, value));
    }
}
