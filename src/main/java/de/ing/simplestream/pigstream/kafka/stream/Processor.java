package de.ing.simplestream.pigstream.kafka.stream;

public interface Processor<T> {
	
	T process(T item);

}
