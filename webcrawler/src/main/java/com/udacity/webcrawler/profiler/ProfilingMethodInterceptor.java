package com.udacity.webcrawler.profiler;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Objects;
import java.lang.reflect.InvocationTargetException;

/**
 * A method interceptor that checks whether {@link Method}s are annotated with the {@link Profiled}
 * annotation. If they are, the method interceptor records how long the method invocation took.
 */
final class ProfilingMethodInterceptor implements InvocationHandler {

  private final Clock clock;
  private final Object delegate;
  private final ProfilingState state;

  ProfilingMethodInterceptor(Clock clock, Object delegate, ProfilingState state) {
    this.clock = Objects.requireNonNull(clock);
    this.delegate = delegate;
    this.state = state;
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    /**
     * This method interceptor inspects the called method to see if it is a profiled method. For
     * profiled methods, the interceptor records a start time, then invokes the method using the object
     * that is being profiled. Finally, for profiled methods, the interceptor records how long the
     * method call took using the ProfilingState methods.
     */
    Object invoked;
    boolean profiled = method.getAnnotation(Profiled.class) != null;
    Instant start = clock.instant();
    try {
      invoked = method.invoke(delegate, args);
    } catch (InvocationTargetException ex) {
      throw ex.getCause();
    } finally {
      if (profiled) {
        Duration duration = Duration.between(start, clock.instant());
        state.record(delegate.getClass(), method, duration);
      }
    }

    return invoked;
  }
}
