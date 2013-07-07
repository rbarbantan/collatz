package ro.cornholio.akka;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.actor.UntypedActorFactory;
import akka.routing.RoundRobinRouter;
import akka.util.Duration;

import java.util.concurrent.TimeUnit;

/**
 * Highest Collatz sequence calculator in Akka.
 * @author rares.barbantan
 */
public class Collatz {

    /**
     * Finds the number with the highest Collatz sequence below a given power of two.
     * @param args
     */
    public static void main(String[] args) {
        Collatz collatz = new Collatz();
        long topNumber = (long) Math.pow(2, Double.parseDouble(args[0]));
        System.out.println("Computing highest Collatz sequence for a number smaller than " + topNumber);
        collatz.calculate(16, topNumber);
    }

    // messages
    static class Calculate {
    }

    static class Work {
        private final long number;

        public Work(long number) {
            this.number = number;
        }

        public long getNumber() {
            return number;
        }
    }

    static class Result {
        private final long number;
        private final long terms;

        public Result(long number, long terms) {
            this.number = number;
            this.terms = terms;
        }

        long getNumber() {
            return number;
        }

        long getTerms() {
            return terms;
        }
    }

    static class FinalResult {
        private final Result result;
        private final Duration duration;

        public FinalResult(Result result, Duration duration) {
            this.result = result;
            this.duration = duration;
        }

        Result getResult() {
            return result;
        }

        public Duration getDuration() {
            return duration;
        }
    }

    /**
     * Slave : does all computing.
     */
    public static class Worker extends UntypedActor {

        private long calculateCollatz(final long number, long terms) {
            long newTerms = 0;
            if(number == 1){
                newTerms = terms;
            }else{
                if(number%2 == 0) {
                    newTerms = calculateCollatz(number / 2, ++terms);
                }else {
                    newTerms = calculateCollatz(3 * number + 1, ++terms);
                }
            }
            return newTerms;
        }


        public void onReceive(Object message) {
            if (message instanceof Work) {
                Work work = (Work) message;
                long terms = calculateCollatz(work.getNumber(), 0);
                getSender().tell(new Result(work.getNumber(),terms), getSelf());
            } else {
                unhandled(message);
            }
        }
    }

    /**
     * Master: distributes work.
     */
    public static class Master extends UntypedActor {
        private final long topNumber;

        private long nrOfResults;
        private Result finalResult = new Result(0,0);

        private final long start = System.currentTimeMillis();

        private final ActorRef listener;
        private final ActorRef workerRouter;

        public Master(final int nrOfWorkers, final long topNumber, ActorRef listener) {
            this.topNumber = topNumber;
            this.listener = listener;

            workerRouter = this.getContext().actorOf(new Props(Worker.class).withRouter(
                    new RoundRobinRouter(nrOfWorkers)),"workerRouter");
        }

        public void onReceive(Object message) {
            if (message instanceof Calculate) {
                for (long start = 1; start <= topNumber; start++) {
                    workerRouter.tell(new Work(start), getSelf());
                }
            } else if (message instanceof Result) {
                Result result = (Result) message;


                if(result.getTerms() > finalResult.getTerms()) {
                    finalResult = result;
                }
                nrOfResults += 1;
                if (nrOfResults == topNumber) {
                    // Send the result to the listener
                    Duration duration = Duration.create(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS);
                    listener.tell(new FinalResult(finalResult, duration), getSelf());
                    // Stops this actor and all its supervised children
                    getContext().stop(getSelf());
                }
            } else {
                unhandled(message);
            }
        }
    }

    /**
     * Prints final result and measurements.
     */
    public static class Listener extends UntypedActor {
        public void onReceive(Object message) {
            if (message instanceof FinalResult) {
                FinalResult fResult = (FinalResult) message;
                System.out.println(String.format("\n\tHighest Collatz sequence: \t\t%s : %s\n\tCalculation time: \t%s",
                        fResult.getResult().getNumber(), fResult.getResult().getTerms(), fResult.getDuration()));
                getContext().system().shutdown();
            } else {
                unhandled(message);
            }
        }
    }

    /**
     * Initializes actor system and calculation.
     * @param nrOfWorkers  how many workers are needed
     * @param topNumber number for which we do calculations
     */
    public void calculate(final int nrOfWorkers, final long topNumber) {
        // Create an Akka system
        ActorSystem system = ActorSystem.create("CollatzSystem");

        // create the result listener, which will print the result and shutdown the system
        final ActorRef listener = system.actorOf(new Props(Listener.class), "listener");

        // create the master
        ActorRef master = system.actorOf(new Props(new UntypedActorFactory() {
            public UntypedActor create() {
                return new Master(nrOfWorkers, topNumber, listener);
            }
        }), "master");

        // start the calculation
        master.tell(new Calculate());

    }
}