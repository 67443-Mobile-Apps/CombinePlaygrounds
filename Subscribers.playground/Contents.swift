import Foundation
import Combine

//MARK: Subscriber Examples
// Not a comprehensive list of subscribers

// `sink` and `assign(to:on:)` return an AnyCancellable. The subscription lives
// only as long as that AnyCancellable does, so we keep every one of them in this set.
var cancellables = Set<AnyCancellable>()

// Sink -> The most commonly used subscriber that provides closures for handling values and completion:
let publisher = [1, 2, 3, 4, 5].publisher

// Basic sink with value handling only
publisher.sink { value in
  print("Received: \(value)")
}
.store(in: &cancellables)

// Sink with completion and value handling
publisher.sink(
  receiveCompletion: { completion in
    switch completion {
    case .finished:
        print("Publisher finished successfully")
    case .failure(let error):
        print("Publisher failed with error: \(error)")
    }
  },
  receiveValue: { value in
    print("Received value: \(value)")
  }
)
.store(in: &cancellables)


// Assign -> Assigns received values to a property of an object using a key path:
class DataModel: ObservableObject {
  @Published var count: Int = 0
  @Published var message: String = ""
}

let model = DataModel()
let numberPublisher = [10, 20, 30].publisher
let stringPublisher = Just("nuqneH Terran!")

// Assign to properties
// CAUTION: assign(to:on:) keeps a strong reference to the object it assigns to.
// That's fine here, but inside a class, `.assign(to: \.x, on: self)` stored in
// self's own cancellables creates a retain cycle and self is never released.
// Use `assign(to: &$x)` (below) instead.
numberPublisher.assign(to: \.count, on: model)
  .store(in: &cancellables)
stringPublisher.assign(to: \.message, on: model)
  .store(in: &cancellables)

print(model.count)   // Will be 30 after all values are processed
print(model.message) // Will be "nuqneH Terran!"


// Assign to Published Property ->
// A convenient way to assign to `@Published` properties.
// The subscription is tied to the @Published property itself, so there is
// no AnyCancellable to store and no retain cycle.
class ViewModel: ObservableObject {
  @Published var searchText: String = ""
  @Published var results: [String] = []

  init() {
    // Assign directly to @Published property
    Just("Initial search")
        .assign(to: &$searchText)

    // Chain operations and assign
    $searchText
        .debounce(for: .milliseconds(500), scheduler: RunLoop.main)
        .map { text in
            // Simulate search results
            ["Result 1 for \(text)", "Result 2 for \(text)"]
        }
        .assign(to: &$results)
  }
}

let viewModel = ViewModel()

viewModel.$results
  .sink { print("Results: \($0)") }
  .store(in: &cancellables)

viewModel.searchText = "Combine"
// Prints immediately: Results: []
// Then ~500ms later:  Results: ["Result 1 for Combine", "Result 2 for Combine"]
// "Initial search" never produces results: debounce drops it because
// "Combine" arrives less than 500ms later.
