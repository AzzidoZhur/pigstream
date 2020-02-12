package de.ing.simplestream.pigstream.kafka.stream;

public interface Writer<T> {
	
	void write(T item);

}
