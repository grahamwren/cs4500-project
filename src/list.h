#pragma once

#include <iostream>

using namespace std;

/**
 * List: a dynamically scaling list
 * authors: @grahamwren, @jagen31
 */
template <typename T> class List {
  int initial_size = 0;
  int allocated_size = 0;
  int length = 0;
  T *data = nullptr; // OWNED

  /**
   * copy data between start and end into dest
   */
  void copy(int start, int end, int dest);

  /**
   * allocate alloc_size of data and copy current data over
   */
  void alloc_data(int alloc_size);

  /**
   * resolve a relative index in this List
   */
  int resolve_index(int i) const;

public:
  /**
   * default constructor, uses default initial_size
   */
  List() = default;

  /**
   * construct a list with the given initial capacity
   */
  List(int initial_size);

  /* copy constructor */
  List(const List<T> &other);
  /* move constructor */
  List(List<T> &&other);

  ~List();
  int size() const;

  /**
   * push: append an element to list.
   *
   * @effect removes value at new size(), decreases size() by 1
   */
  void push(T e);

  /**
   * pop: remove last element from list
   *
   * @effect adds value at old size(), increases size() by 1
   */
  T pop();

  /**
   * get: get the element at the requested index
   */
  T &get(int rel_i);

  /**
   * set: replace the element at i with e then return the replaced element, if
   *      requested index is outside the current list, assumes index is
   *      relative to length of the list.
   * @effect mutates this, size() unchanged
   */
  T set(int rel_i, T e);

  /**
   * move: move n elements from the from_i index to the to_i index
   *
   * @effect moves n elements from rel_from_i to rel_to_i, size() unchanged
   */
  void move(int n, int rel_from_i, int rel_to_i);

  /**
   * swaps one element with another
   *
   * @effect swaps elements, size() unchanged
   */
  void swap(int rel_from_i, int rel_to_i);

  /**
   * find: return the index of the first occurrence of the element
   *       which is `==` to the given object. If not found
   *       returns `size() + 1`
   */
  int find(T o) const;

  /**
   * clear: empties the list
   *
   * @effect size() = 0
   */
  void clear();

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
  void ensure_space(int requested_size);

  /**
   * print a debug representation of this object
   */
  void debug() const;

  /**
   * compare these two lists for equality using the given comparator lambda
   *
   * comparator defaults to `==` equality
   */
  bool equals(
      const List<T> &,
      function<bool(T, T)> = [](T l, T r) { return l == r; }) const;
};
