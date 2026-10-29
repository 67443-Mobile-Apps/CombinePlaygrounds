import Foundation
import Combine

// Note: These are popular operators, but not at all comprehensive...

// `sink` returns an AnyCancellable. The subscription lives only as long as
// that AnyCancellable does, so we keep every one of them in this set.
var cancellables = Set<AnyCancellable>()

//MARK: Some Transforming Operators

// .map ->Transforms each value using a closure:
let numbers = [1, 2, 3, 4, 5].publisher

numbers
    .map { $0 * 2 }
    .sink { print($0) }
    .store(in: &cancellables)
// Prints: 2, 4, 6, 8, 10


// .tryMap -> Like map, but allows throwing errors:
enum ValidationError: Error {
    case negativeNumber
}

let numbers2 = [2, 4, -1, 6].publisher

numbers2
    .tryMap { value -> Int in
        guard value >= 0 else {
            throw ValidationError.negativeNumber
        }
        return value * 2
    }
    .sink(
        receiveCompletion: { completion in
            switch completion {
            case .finished:
                print("Completed successfully")
            case .failure(let error):
                print("Error: \(error)")
            }
        },
        receiveValue: { print("Value: \($0)") }
    )
    .store(in: &cancellables)
// Prints: Value: 4, Value: 8, Error: negativeNumber
// Note: 6 is never processed because the error ends the stream


// .compactMap -> Transforms and filters out nil values:

let strings = ["1", "hello", "3", "world", "5"].publisher

strings
    .compactMap { Int($0) }
    .sink { print($0) }
    .store(in: &cancellables)
// Prints: 1, 3, 5


// .scan -> Provides the current value along with the previous result:
let numbers3 = [1, 2, 3, 4, 5].publisher

numbers3
    .scan(0) { accumulator, value in
        accumulator + value
    }
    .sink { print($0) }
    .store(in: &cancellables)
// Prints: 1, 3, 6, 10, 15 (running sum)


//MARK: Some Filtering Operators

// .filter -> Filters values based on a predicate:
let numbers4 = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10].publisher

numbers4
    .filter { $0 % 2 == 0 }
    .sink { print($0) }
    .store(in: &cancellables)
// Prints: 2, 4, 6, 8, 10


// .removeDuplicates -> Removes consecutive duplicate values:
let values = [1, 1, 2, 2, 2, 3, 1, 1].publisher

values
    .removeDuplicates()
    .sink { print($0) }
    .store(in: &cancellables)
// Prints: 1, 2, 3, 1


// first and last -> Emits only the first or last value:
let numbers5 = [1, 2, 3, 4, 5].publisher

// First value
numbers5
    .first()
    .sink { print("First: \($0)") }
    .store(in: &cancellables)
// Prints: First: 1

// Last value
numbers5
    .last()
    .sink { print("Last: \($0)") }
    .store(in: &cancellables)
// Prints: Last: 5

// First value matching condition
numbers5
    .first { $0 > 3 }
    .sink { print("First > 3: \($0)") }
    .store(in: &cancellables)
// Prints: First > 3: 4


// prefix and drop -> Control how many values to take or skip:
let numbers6 = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10].publisher

// Take first 3 values
numbers6
    .prefix(3)
    .sink { print("Prefix: \($0)") }
    .store(in: &cancellables)
// Prints: Prefix: 1, Prefix: 2, Prefix: 3

// Skip first 3 values
numbers6
    .dropFirst(3)
    .sink { print("Drop: \($0)") }
    .store(in: &cancellables)
// Prints: Drop: 4, Drop: 5, Drop: 6, Drop: 7, Drop: 8, Drop: 9, Drop: 10


//MARK: Combining Operators

//  merge -> Combines values from multiple publishers of the same type:
let publisher1 = PassthroughSubject<Int, Never>()
let publisher2 = PassthroughSubject<Int, Never>()

Publishers.Merge(publisher1, publisher2)
    .sink { print("Merged: \($0)") }
    .store(in: &cancellables)

publisher1.send(1)
publisher2.send(2)
publisher1.send(3)
// Prints: Merged: 1, Merged: 2, Merged: 3


// .combineLatest -> Combines the latest values from multiple publishers:
let namePublisher = PassthroughSubject<String, Never>()
let agePublisher = PassthroughSubject<Int, Never>()

namePublisher
    .combineLatest(agePublisher)
    .sink { name, age in
        print("Latest: \(name), \(age)")
    }
    .store(in: &cancellables)

namePublisher.send("Alice")
agePublisher.send(30)      // Prints: Latest: Alice, 30
namePublisher.send("Bob")  // Prints: Latest: Bob, 30
agePublisher.send(25)      // Prints: Latest: Bob, 25


// .zip -> Pairs values from multiple publishers in order:
let letters = ["A", "B", "C", "D"].publisher
let numbers7 = [1, 2, 3].publisher

letters
    .zip(numbers7)
    .sink { letter, number in
        print("Zipped: \(letter)\(number)")
    }
    .store(in: &cancellables)
// Prints: Zipped: A1, Zipped: B2, Zipped: C3
// Note: D is not printed because numbers only has 3 values


// prepend and append -> Add values to the beginning or end of a stream:
let numbers8 = [2, 3, 4].publisher

numbers8
    .prepend(1)
    .append(5)
    .sink { print($0) }
    .store(in: &cancellables)
// Prints: 1, 2, 3, 4, 5


//MARK: Sequence Operators

// .collect ->Collects all values into an array:
let numbers9 = [1, 2, 3, 4, 5].publisher

numbers9
    .collect()
    .sink { array in
        print("Collected: \(array)")
    }
    .store(in: &cancellables)
// Prints: Collected: [1, 2, 3, 4, 5]

// Collect in batches
numbers9
    .collect(3)
    .sink { batch in
        print("Batch: \(batch)")
    }
    .store(in: &cancellables)
// Prints: Batch: [1, 2, 3], Batch: [4, 5]


// .reduce -> Reduces all values to a single value:
let numbers10 = [1, 2, 3, 4, 5].publisher

numbers10
    .reduce(0) { accumulator, value in
        accumulator + value
    }
    .sink { total in
        print("Sum: \(total)")
    }
    .store(in: &cancellables)
// Prints: Sum: 15


// .contains -> Checks if the publisher contains a specific value:
let numbers11 = [1, 2, 3, 4, 5].publisher

numbers11
    .contains(3)
    .sink { contains in
        print("Contains 3: \(contains)")
    }
    .store(in: &cancellables)
// Prints: Contains 3: true


//MARK: Timing Operators

// .delay -> Delays the emission of values:

let publisher = ["A", "B", "C"].publisher

publisher
    .delay(for: .seconds(1), scheduler: RunLoop.main)
    .sink { print("Delayed: \($0)") }
    .store(in: &cancellables)
// Values are printed 1 second later


// .debounce -> Waits for a pause in emissions before emitting the latest value:
let searchTerms = PassthroughSubject<String, Never>()

searchTerms
    .debounce(for: .milliseconds(500), scheduler: RunLoop.main)
    .sink { term in
        print("Searching for: \(term)")
    }
    .store(in: &cancellables)

searchTerms.send("a")
searchTerms.send("ap")
searchTerms.send("app")
searchTerms.send("appl")
searchTerms.send("apple")
// Only prints "Searching for: apple" after 500ms pause


// .throttle -> Limits the rate of emissions:
let rapidPublisher = Timer.publish(every: 0.1, on: .main, in: .common)
    .autoconnect()

rapidPublisher
    .throttle(for: .seconds(1), scheduler: RunLoop.main, latest: true)
    .prefix(5)
    .sink { _ in
        print("Throttled emission at \(Date())")
    }
    .store(in: &cancellables)
// Prints once per second despite rapid timer (stops after 5 with .prefix)
