#ifndef _POOL_TS_H_20141129_

#define _POOL_TS_H_20141129_

#include <limits>
#include <vector>
#include <atomic>
#include <persist/storage/types.h>
#include <vector>
//#include <rabbit/unordered_map>
#include <mutex>
//#include <storage/transactions/system_timers.h>

#include <typeinfo>
#include <unordered_map>

extern void add_btree_totl_used(ptrdiff_t added);

extern void remove_btree_totl_used(ptrdiff_t added);

#ifdef _MSC_VER
#define thread_local __declspec(thread)
#endif
#ifndef TRUE
#define FALSE 0
#define TRUE !FALSE
#endif
namespace persist {
    namespace storage {
        namespace unordered = std;
        typedef std::lock_guard<std::recursive_mutex> synchronized;
        typedef std::lock_guard<std::mutex> f_synchronized;

        extern void add_buffer_use(long long added);

        extern void remove_buffer_use(long long removed);

        extern void add_total_use(long long added);

        extern void remove_total_use(long long removed);

        extern void add_col_use(long long added);

        extern void remove_col_use(long long removed);

        extern void add_stl_use(long long added);

        extern void remove_stl_use(long long added);

        extern long long total_use;
        extern long long buffer_use;
        extern long long col_use;
        extern long long stl_use;

        namespace allocation {
            struct bt_counter {
                void add(size_t bytes) {
                    add_btree_totl_used(bytes);
                };

                void remove(size_t bytes) {
                    remove_btree_totl_used(bytes);
                };
            };

            struct stl_counter {

                void add(size_t bytes) {
                    add_stl_use(bytes);
                };

                void remove(size_t bytes) {
                    remove_stl_use(bytes);
                };
            };

            struct col_counter {
                void add(size_t bytes) {
                    add_col_use(bytes);
                };

                void remove(size_t bytes) {
                    remove_col_use(bytes);
                };
            };

            struct buffer_counter {
                void add(size_t) {
                    //add_buffer_use(bytes);
                };

                void remove(size_t) {
                    //remove_buffer_use(bytes);
                };
            };

            /// set to true to enable heap corruption checking and delete order checking
            static const bool heap_alloc_check = false;
            /// derived from The C++ Standard Library - A Tutorial and Reference - Nicolai M. Josuttis
            /// the bean counting issue

            template<class T>
            class base_tracker {
            public:

            public:
                // type definitions

                typedef T value_type;
                typedef T *pointer;
                typedef const T *const_pointer;
                typedef T &reference;
                typedef const T &const_reference;
                typedef std::size_t size_type;
                typedef std::ptrdiff_t difference_type;

                // rebind allocator to type U
                template<class U>
                struct rebind {
                    typedef base_tracker<U> other;
                };

                // return address of values
                pointer address(reference value) const {
                    return &value;
                }

                const_pointer address(const_reference value) const {
                    return &value;
                }

                /* constructors and destructor
                * - nothing to do because the allocator has no state
                */
                base_tracker() throw() {
                }

                base_tracker(const base_tracker &) throw() {
                }

                template<class U>
                base_tracker(const base_tracker<U> &) throw() {
                }

                ~base_tracker() throw() {
                }

                // a guess of the overhead that malloc may incur per allocation
                size_type overhead() const throw() {
                    return sizeof(void *);
                }

                // return maximum number of elements that can be allocated
                size_type max_size() const throw() {
                    return ((std::numeric_limits<size_t>::max)()) / sizeof(T);
                }

                // allocate but don't initialize num elements of type T
                pointer allocate(size_type num, const void * = 0) {
                    // print message and allocate memory with global new

                    pointer ret = (pointer) (malloc(num * sizeof(T)));

                    return ret;
                }

                /// for 'dense' hash map
                pointer reallocate(pointer p, size_type n) {

                    return static_cast<pointer>(realloc(p, n * sizeof(value_type)));
                }

                // initialize elements of allocated storage p with value value
                void construct(T *p, const T &value) {
                    // initialize memory with placement new
                    new((void *) p)T(value);
                }

                // initialize elements with variyng args
#ifndef _MSC_VER

                template<typename... _Args>
                void construct(T *p, _Args &&... __args) {
                    new((void *) p)T(std::forward<_Args>(__args)...);
                }

#endif

                // destroy elements of initialized storage p
                void destroy(pointer p) {
                    // destroy objects by calling their destructor
                    p->~T();
                }

                // deallocate storage p of deleted elements
                void deallocate(pointer p, size_type) {
                    // print message and deallocate memory with global delete
                    free((void *) p);

                }

            };// tracker tracking allocator

            template<class T, class _Counter = stl_counter>
            class tracker {
            public:

            public:
                // type definitions

                typedef T value_type;
                typedef T *pointer;
                typedef const T *const_pointer;
                typedef T &reference;
                typedef const T &const_reference;
                typedef std::size_t size_type;
                typedef std::ptrdiff_t difference_type;
                typedef _Counter counter_type;
                // rebind allocator to type U
                template<class U>
                struct rebind {
                    typedef tracker<U> other;
                };

                // return address of values
                pointer address(reference value) const {
                    return &value;
                }

                const_pointer address(const_reference value) const {
                    return &value;
                }

                /* constructors and destructor
                * - nothing to do because the allocator has no state
                */
                tracker() throw() {
                }

                tracker(const tracker &) throw() {
                }

                template<class U>
                tracker(const tracker<U> &) throw() {
                }

                ~tracker() throw() {
                }

                size_type overhead() const throw() {
                    return sizeof(void *);
                }

                // return maximum number of elements that can be allocated
                size_type max_size() const throw() {
                    return ((std::numeric_limits<size_t>::max)()) / sizeof(T);
                }

                // allocate but don't initialize num elements of type T
                pointer allocate(size_type num, const void * = 0) {
                    pointer ret = (pointer) (malloc(num * sizeof(T)));
                    counter_type c;
                    c.add(num * sizeof(T) + overhead());
                    return ret;
                }

                // deallocate storage p of deleted elements
                void deallocate(pointer p, size_type num) {
                    free((void *) p);
                    counter_type c;
                    c.remove(num * sizeof(T));
                }

                /// for 'dense' hash map
                pointer reallocate(pointer p, size_type n) {

                    return static_cast<pointer>(realloc(p, n * sizeof(value_type)));
                }

                // initialize elements of allocated storage p with value value
                void construct(pointer p, const T &value) {
                    // initialize memory with placement new
                    new((void *) p)T(value);
                }

                // initialize elements with variyng args
#ifndef _MSC_VER

                template<typename... _Args>
                void construct(T *p, _Args &&... __args) {
                    new((void *) p)T(std::forward<_Args>(__args)...);
                }

#endif

                // destroy elements of initialized storage p
                void destroy(pointer p) {
                    // destroy objects by calling their destructor
                    p->~T();
                }


            };// tracker tracking allocator
            // return that all specializations of this allocator are interchangeable
            template<class T1, class T2>
            bool operator==(const tracker<T1> &, const tracker<T2> &) throw() {
                return true;
            }

            template<class T1, class T2>
            bool operator!=(const tracker<T1> &, const tracker<T2> &) throw() {
                return false;
            }

            template<class T>
            class pool_tracker : public base_tracker<T> {
            public:
                /// this is realy *realy* silly, isnt the types inherited as well ?? someone deserves a rasberry
                // type definitions
                typedef base_tracker<T> base_class;
                typedef T value_type;
                typedef T *pointer;
                typedef const T *const_pointer;
                typedef T &reference;
                typedef const T &const_reference;
                typedef std::size_t size_type;
                typedef std::ptrdiff_t difference_type;

                // rebind allocator to type U

                template<class U>
                struct rebind {
                    typedef pool_tracker<U> other;
                };

                pool_tracker() throw() {
                }

                pool_tracker(const pool_tracker &) throw() {
                }

                template<class U>
                pool_tracker(const pool_tracker<U> &) throw() {}

                // allocate but don't initialize num elements of type T
                pointer allocate(size_type num, const void * = 0) {
                    stl_counter c;
                    c.add(num * sizeof(T) + this->overhead());
                    return base_class::allocate(num);
                }

                // deallocate storage p of deleted elements
                void deallocate(pointer p, size_type num) {
                    base_class::deallocate(p, num);
                    stl_counter c;
                    c.remove(num * sizeof(T) + this->overhead());
                }
            };

            class pool_shared {
            public:
                pool_shared() {
                    allocated = 0;
                    used = 0;
                    instances = 0;
                    current = 0;

                }

                std::atomic<u64> allocated;
                std::atomic<u64> used;
                std::atomic<u64> instances;
                std::atomic<u64> current;

            };


        };
    };
};
/// short hand for the namespace
namespace sta = ::persist::storage::allocation;

#endif /// _POOL_TS_H_20141129_
