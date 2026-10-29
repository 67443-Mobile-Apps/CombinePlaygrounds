import Foundation
import Combine

//MARK: Publisher Examples
// Not a comprehensive list of publishers

// `sink` returns an AnyCancellable. The subscription lives only as long as
// that AnyCancellable does, so we keep every one of them in this set.
var cancellables = Set<AnyCancellable>()

// Just -> Emits a single value and then completes:
let justPublisher = Just("Hello, World!")

justPublisher.sink { value in
  print(value) // Prints: Hello, World!
}
.store(in: &cancellables)


// Empty -> Completes immediately without emitting any values:
let emptyPublisher = Empty<String, Never>()

emptyPublisher.sink(
  receiveCompletion: { _ in print("Completed") },
  receiveValue: { print($0) }
)  // Only prints: Completed
.store(in: &cancellables)


// Sequence -> Converts a sequence into a publisher:
let sequencePublisher = [1, 2, 3, 4, 5].publisher

sequencePublisher.sink { value in
  print(value) // Prints 1, 2, 3, 4, 5
}
.store(in: &cancellables)


// Timer -> Emits values on a regular interval:
let timerPublisher = Timer.publish(every: 1.0, on: .main, in: .common)
    .autoconnect()

timerPublisher.sink { date in
  print("Timer fired at: \(date)")
}
.store(in: &cancellables)


// Future -> For wrapping async operations:
func fetchData() -> Future<String, Error> {
  return Future { promise in
    DispatchQueue.global().asyncAfter(deadline: .now() + 2) {
      promise(.success("Data loaded"))
    }
  }
}

fetchData().sink(
  receiveCompletion: { _ in },
  receiveValue: { print($0) }
)
.store(in: &cancellables)


// PassthroughSubject -> A subject that broadcasts values to multiple subscribers:

let subject = PassthroughSubject<String, Never>()

subject.sink { value in
  print("Subscriber 1: \(value)")
}
.store(in: &cancellables)

subject.sink { value in
  print("Subscriber 2: \(value)")
}
.store(in: &cancellables)

subject.send("Hello")
subject.send("World")
// Both subscribers receive both values


//MARK: Creating Custom Publisher
struct CountdownPublisher: Publisher {
  typealias Output = Int
  typealias Failure = Never
  
  let start: Int
  
  func receive<S>(subscriber: S) where S: Subscriber,
                  S.Input == Output, S.Failure == Failure {
    let subscription = CountdownSubscription(
        subscriber: subscriber,
        start: start
    )
    subscriber.receive(subscription: subscription)
  }
}

class CountdownSubscription<S: Subscriber>: Subscription
  where S.Input == Int, S.Failure == Never {
  
  private var subscriber: S?
  private var current: Int
  
  init(subscriber: S, start: Int) {
    self.subscriber = subscriber
    self.current = start
  }
  
  func request(_ demand: Subscribers.Demand) {
    guard let subscriber = subscriber else { return }
    
    while current > 0 && demand > 0 {
      _ = subscriber.receive(current)
      current -= 1
    }
      
    if current <= 0 {
      subscriber.receive(completion: .finished)
    }
  }
  
  func cancel() {
    subscriber = nil
  }
}

// Usage
CountdownPublisher(start: 3).sink { value in
  print(value) // Prints: 3, 2, 1
}
.store(in: &cancellables)

/*
 SUMMARY - DIFFERENT KINDS OF PUBLISHERS
 ---------------------------------------
 - `Just(5)` emits one value, then finishes.
 - `[1, 2, 3].publisher` emits each element of the array.
 - `URLSession.shared.dataTaskPublisher(for:)` emits a network response.
 - `NotificationCenter.default.publisher(for:)` emits notifications.
 - `@Published var name: String` gives you `$name`, a publisher of changes to that property.
 - `PassthroughSubject` / `CurrentValueSubject` are publishers you push values into by hand.
 
 Publishers are **lazy**: nothing happens until something subscribes.
 
*/
