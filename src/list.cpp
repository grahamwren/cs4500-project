#include "list.h"

List::List(int initial_size) : initial_size(initial_size) {
  ensure_space(initial_size);
}

List::List(const List<T> &other) : List(other.size()) {
  for (int i = 0; i < other.size(); i++) {
    push(other.get(i));
  }
}

List::List(List<T> &&other)
    : initial_size(other.initial_size), allocated_size(other.allocated_size),
      size(other.size), data(other.data) {
  other.data = nullptr;
}

~List() { delete[] data; }

int size() const { return length; }

/**
 * push: append an element to list.
 *
 * @effect removes value at new size(), decreases size() by 1
 */
void List::push(T e) {
  ensure_space(size + 1); // ensure space for one more val
  hash_ = 0;              // reset cached hash
  size++;                 // increase the length
  set(-1, e);             // set last element to e
}

/**
 * pop: remove last element from list
 *
 * @effect adds value at old size(), increases size() by 1
 */
T List::pop() {
  assert(size > 0); // at least one element in list
  hash_ = 0;        // reset cached hash
  T e = get(-1);    // get last element
  size--;           // decrement size
  return e;
}

/**
 * get: get the element at the requested index
 */
T const &List::get(int rel_i) const {
  assert(size > 0);                 // cannot get in empty list
  int index = resolve_index(rel_i); // resolve relative index
  assert(0 <= index &&
         index < size); // assert index is valid TODO: (@grahamwren) remove
                        // assert once trust resolve_index
  return data[index];
}

/**
 * set: replace the element at i with e then return the replaced element, if
 *      requested index is outside the current list, assumes index is
 *      relative to length of the list.
 * @effect mutates this, size() unchanged
 */
T List::set(int rel_i, T e) {
  assert(size > 0);                 // cannot set in empty list
  int index = resolve_index(rel_i); // resolve relative index
  assert(0 <= index &&
         index < size); // assert index is valid TODO: (@grahamwren) remove
                        // assert once trust resolve_index
  hash_ = 0;            // mutates this so reset cached hash
  T old = get(index);   // get element being replaced
  data[index] = e;      // set index to e
  return old;           // return old element
}

/**
 * move: move n elements from the from_i index to the to_i index
 *
 * @effect moves n elements from rel_from_i to rel_to_i, size() unchanged
 */
void List::move(int n, int rel_from_i, int rel_to_i) {
  int from_i = resolve_index(rel_from_i);
  int to_i = resolve_index(rel_to_i);

  // assert this is a valid move within the list
  assert(from_i + n <= size && to_i + n <= size);

  // return if nothing to do
  if (from_i == to_i)
    return;

  // something to do, reset hash
  hash_ = 0; // reset cached hash

  // copy chunk to be moved into temp array
  T *temp = new T[n];
  for (int i = 0; i < n; i++) {
    temp[i] = get(from_i + i);
  }

  if (from_i < to_i) { // moving right
    // copy all data starting after `from_i + n` until `to_i + n` into
    // `from_i`
    copy(from_i + n, to_i + n, from_i);
  } else { // moving left
    // copy all data starting at `to_i` until `from_i + n` into `to_i + n`
    copy(to_i, from_i + n, to_i + n);
  }

  // copy temp into `to_i`
  for (int i = 0; i < n; i++) {
    set(to_i + i, temp[i]);
  }

  // free temp
  delete[] temp;
}

void List::copy(int start, int end, int dest) {
  int chunk_len = end - start;
  T *chunk = new T[chunk_len];
  for (int i = 0; i < chunk_len; i++) {
    chunk[i] = get(start + i);
  }
  for (int i = 0; i < chunk_len; i++) {
    if (dest + i >= size())
      continue;
    set(dest + i, chunk[i]);
  }
  delete[] chunk;
}

/**
 * swaps one element with another
 *
 * @effect swaps elements, size() unchanged
 */
void List::swap(int rel_from_i, int rel_to_i) {
  hash_ = 0;                      // reset hash because mutation
  T e = get(rel_to_i);            // get dest element
  set(rel_to_i, get(rel_from_i)); // set src element at dest position
  set(rel_from_i, e);             // set dest element at src position
}

/**
 * find: return the index of the first occurance of the element
 *       which is `==` to the given object. If not found
 *       returns `size() + 1`
 */
int List::find(T o) const {
  int len = size();
  for (int i = 0; i < len; i++) {
    T e = get(i);
    if (o == e)
      return i;
  }
  return len + 1;
};

/**
 * clear: empties the list
 *
 * @effect size() = 0
 */
void List::clear() {
  size = 0;                 // reset size
  hash_ = 0;                // reset cached hash
  alloc_data(initial_size); // re-allocate empty data (unnecessary)
}

/**
 * ensure_space: ensure there is space for the requested size, if not
 *               allocate more. This method is designed for when a user of
 *               this List wants to push multiple elements without triggering
 *               re-allocates. This will allocate the requested space ahead
 *               of time.
 *
 * @effect pre-allocates the requested amount of space all-at-once, size()
 *         unchanged
 */
void List::ensure_space(int requested_size) {
  if (requested_size == 0)
    return; // do NOT allocate if requesting zero space
  if (requested_size > allocated_size) {
    // allocate ~50% more space than requested
    int alloc_size = (fmax(requested_size, 2) / 2) * 3;
    alloc_data(alloc_size);
  }
}

void List::alloc_data(int alloc_size) {
  assert(alloc_size <= size); // prevent allocation smaller than data
  T *old = data;
  data = new T[alloc_size];
  allocated_size = alloc_size;

  if (old != nullptr) {
    for (int i = 0; i < size; i++) {
      data[i] = old[i];
    }
    delete[] old;
  }
}

int List::resolve_index(int i) const {
  int len = size();
  int index = i % len;
  if (index >= 0) // if positive just return it
    return index;
  return len + index; // else negative, treat as dist from end
}

void List::debug() const {
  int len = size();
  cout << "List<" << (void *)this << ">(size: " << len << ", "
       << "entries: [";
  if (len > 0) {
    cout << get(0);
    for (int i = 1; i < len; i++) {
      cout << ", " << get(i);
    }
  }
  cout << "])";
}

bool List::equals(const List<T> &other, function<bool(T, T)> comparator) const {
  if (size() != other.size())
    return false;

  bool equal = true;
  for (int i = 0; i < size(); i++) {
    if (!comparator(get(i), other.get(i))) {
      equal = false;
      break;
    }
  }
  return equal;
}

// explicit instantiation
List<Column *>;
List<int>;
List<float>;
List<bool>;
List<string *>;
