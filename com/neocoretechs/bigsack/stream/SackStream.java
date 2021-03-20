package com.neocoretechs.bigsack.stream;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;
import java.util.stream.BaseStream;
import java.util.stream.Collector;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Lynchpin superclass of all BigSack streams that wraps the Iterator with Spliterator using:
 * (Spliterator<T>) Spliterators.spliteratorUnknownSize(esi, characteristics) <p/>
 * Once wrapped, the system supplied StreamSupport class provides the actual stream using:
 * (Stream<T>) StreamSupport.stream(spliterator, parallel) <p/>
 * Before calling 'of()', call 'parallel()' , 'setParallel(true | false)', 'sequential()' or 
 * 'unordered()' to change the charactaristics of the stream.<p/>
 * The default charactaristscs are Spliterator.DISTINCT | Spliterator.SORTED | Spliterator.ORDERED<p/>
 * @author Jonathan Groff Copyright (C) NeoCoreTechs 2021
 *
 * @param <T>
 */
public class SackStream<T> implements Stream<T> {
	protected static final int characteristics = Spliterator.DISTINCT | Spliterator.SORTED | Spliterator.ORDERED;
	protected static final int characteristicsUnordered = Spliterator.DISTINCT | Spliterator.SORTED ;
	Spliterator<T> spliterator;
	boolean parallel = false;
	Iterator<T> it;
		
	public SackStream(Iterator esi) {
		this.it = esi;
		spliterator = (Spliterator<T>) Spliterators.spliteratorUnknownSize(esi, characteristics);
	}
	
	public Stream<T> of() {
	    return (Stream<T>) StreamSupport.stream(spliterator, parallel);
	}
	
	public void setParallel(boolean parallel) { this.parallel = parallel; }
	
	@Override
	public Stream<T> filter(Predicate predicate) {
		return of().filter(predicate);
	}

	@Override
	public Stream map(Function mapper) {
		return of().map(mapper);
	}

	@Override
	public IntStream mapToInt(ToIntFunction mapper) {
		return of().mapToInt(mapper);
	}

	@Override
	public LongStream mapToLong(ToLongFunction mapper) {
		return of().mapToLong(mapper);
	}

	@Override
	public DoubleStream mapToDouble(ToDoubleFunction mapper) {
		return of().mapToDouble(mapper);
	}

	@Override
	public Stream flatMap(Function mapper) {
		return of().flatMap(mapper);
	}

	@Override
	public IntStream flatMapToInt(Function mapper) {
		return of().flatMapToInt(mapper);
	}

	@Override
	public LongStream flatMapToLong(Function mapper) {
		return of().flatMapToLong(mapper);
	}

	@Override
	public DoubleStream flatMapToDouble(Function mapper) {
		return of().flatMapToDouble(mapper);
	}

	@Override
	public Stream distinct() {
		return of().distinct();
	}

	@Override
	public Stream sorted() {
		return of();
	}

	@Override
	public Stream sorted(Comparator comparator) {
		return of().sorted(comparator);
	}

	@Override
	public Stream peek(Consumer action) {
		return of().peek(action);
	}

	@Override
	public Stream limit(long maxSize) {
		return of().limit(maxSize);
	}

	@Override
	public Stream skip(long n) {
		return of().skip(n);
	}

	@Override
	public void forEach(Consumer action) {
		of().forEach(action);	
	}

	@Override
	public void forEachOrdered(Consumer action) {
		of().forEachOrdered(action);
	}

	@Override
	public Object[] toArray() {
		return of().toArray();
	}

	@Override
	public Object[] toArray(IntFunction generator) {
		return of().toArray(generator);
	}

	@Override
	public Object reduce(Object identity, BinaryOperator accumulator) {
		return of().reduce((T) identity, accumulator);
	}

	@Override
	public Optional reduce(BinaryOperator accumulator) {
		return of().reduce(accumulator);
	}

	@Override
	public Object reduce(Object identity, BiFunction accumulator, BinaryOperator combiner) {
		return of().reduce(identity, accumulator, combiner);
	}

	@Override
	public Object collect(Supplier supplier, BiConsumer accumulator, BiConsumer combiner) {
		return of().collect(supplier, accumulator, combiner);
	}

	@Override
	public Object collect(Collector collector) {
		return of().collect(collector);
	}

	@Override
	public Optional min(Comparator comparator) {
		return of().min(comparator);
	}

	@Override
	public Optional max(Comparator comparator) {
		return of().max(comparator);
	}

	@Override
	public long count() {
		return of().count();
	}

	@Override
	public boolean anyMatch(Predicate predicate) {
		return of().anyMatch(predicate);
	}

	@Override
	public boolean allMatch(Predicate predicate) {
		return of().allMatch(predicate);
	}

	@Override
	public boolean noneMatch(Predicate predicate) {
		return of().noneMatch(predicate);
	}

	@Override
	public Optional findFirst() {
		return of().findFirst();
	}

	@Override
	public Optional findAny() {
		return of().findAny();
	}

	@Override
	public Iterator iterator() {
		return it;
	}

	@Override
	public Spliterator spliterator() {
		return spliterator;
	}

	@Override
	public boolean isParallel() {
		return parallel;
	}

	@Override
	public Stream<T> sequential() {
		 return (Stream<T>) StreamSupport.stream(spliterator, false);
	}

	@Override
	public Stream<T> parallel() {
		 return (Stream<T>) StreamSupport.stream(spliterator, true);
	}

	@Override
	public Stream<T> unordered() {
		spliterator = Spliterators.spliteratorUnknownSize(it, characteristicsUnordered);
		return of();
	}

	@Override
	public Stream<T> onClose(Runnable closeHandler) {
		return of().onClose(closeHandler);
	}

	@Override
	public void close() {
		of().close();	
	}
	
}
