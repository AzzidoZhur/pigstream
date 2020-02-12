package de.ing.simplestream.pigstream.services;

import de.ing.simplestream.pigstream.kafka.stream.Processor;
import de.ing.simplestream.pigstream.tiere.Schwein;

public class PigProcessor implements Processor<Schwein> {

	@Override
	public Schwein process(Schwein item) {
		item.fressen();
		return item;
	}

}
