// $Id: btree.h 130 2011-05-18 08:24:25Z tb $ -*- fill-column: 79 -*-
// $Id: btree.h 130 2011-05-18 08:24:25Z tb $ -*- fill-column: 79 -*-
/** \file btree.h
* Contains the main B+ tree implementation template class btree.
*/

/*
* persist B+ Tree Storage Template Classes v s.9.0
*/
/*****************************************************************************

Copyright (C) 2008-2011 Timo Bingmann
Copyright (c) 2013-2022, Christiaan Pretorius

*****************************************************************************/
///
/// TODO: There are some const violations inherent to a smart pointer
/// implementation of persisted pages. These are limited to 4 such cases
/// found and explained in the proxy smart pointer template type.
///
/// NOTE: NB: for purposes of sanity the STL operator*() and -> dereference
/// operators on the iterator has been removed so that inter tree concurrent
/// leaf page sharing can be done efficiently. only the key() and data()
/// members remain to access keys and values seperately

#ifndef PERSIST_BTREE_H_
#define PERSIST_BTREE_H_



// *** Required Headers from the STL
#ifdef _MSC_VER
#include <Windows.h>
#endif

#include <algorithm>
#include <functional>

#include <istream>
#include <ostream>
#include <memory>
#include <cstddef>
#include <bitset>
#include <cassert>
#include <vector>
#include <set>
#include <map>
#include <unordered_map>
#include <persist/storage/basic_storage.h>
#include <persist/storage/pool.h>
#include <persist/storage/types.h>
#include <persist/storage/pool.h>
#include <rabbit/unordered_map>

// *** Debugging Macros

#ifdef BTREE_DEBUG

                                                                                                                        #include <iostream>

/// Print out debug information to std::cout if BTREE_DEBUG is defined.
#define BTREE_PRINT(x,...)          do { if (debug) (sprintf(stderr, x, __VA_ARGS__)); } while(0)

/// Assertion only if BTREE_DEBUG is defined. This is not used in verify().
#define BTREE_ASSERT(x)         do { assert(x); } while(0)

#else

/// Print out debug information to std::cout if BTREE_DEBUG is defined.
#define BTREE_PRINT(x, ...)          do { } while(0)

/// Assertion only if BTREE_DEBUG is defined. This is not used in verify().
#define BTREE_ASSERT(x)         do {  } while(0)

#endif
#define BT_CONSTRUCTOR_OPT_ 1
/// std::max function does not work for initializing static const limiters
#ifndef max_const
#define max_const(a, b)            (((a) > (b)) ? (a) : (b))
#endif

// * define an os related realtime clock

#define OS_CLOCK Poco::Timestamp().epochMicroseconds

/// Compiler options for optimization
#ifdef _MSC_VER
#pragma warning(disable:4503)
#endif
#ifdef _MSC_VER
                                                                                                                        #define FORCE_INLINE __forceinline
#define NO_INLINE _declspec(noinline)
#else
#define FORCE_INLINE
#define NO_INLINE
#endif
/// persist - Some Template Extensions namespace

extern ptrdiff_t btree_totl_used;
extern ptrdiff_t btree_totl_instances;
extern ptrdiff_t btree_totl_surfaces;

//extern void add_btree_totl_used(ptrdiff_t added);

//extern void remove_btree_totl_used(ptrdiff_t added);

//class malformed_page_exception : public std::exception {
//public:
//    malformed_page_exception() noexcept {};
//};

//class method_not_implemented_exception : public std::exception {
//public:
//    method_not_implemented_exception() noexcept {};
//};
#if 0
template<typename T>
struct has_typedef_attached_values {
    // Types "yes" and "no" are guaranteed to have different sizes,
    // specifically sizeof(yes) == 1 and sizeof(no) == 2.
    typedef char yes[1];
    typedef char no[2];

    template<typename C>
    static yes &test(typename C::attached_values *);

    template<typename>
    static no &test(...);

    // If the "sizeof" of the result of calling test<T>(0) would be equal to sizeof(yes),
    // the first overload worked and T has a nested type named foobar.
    static const bool value = sizeof(test<T>(nullptr)) == sizeof(yes);
};
#endif
namespace nst = persist::storage;
namespace persist {
    class bad_format : public std::exception {
    public:
        bad_format() {
            print_err("btree bad format");
        };
    };

    class bad_pointer : public std::exception {
    public:
        bad_pointer() {
            print_err("btree bad pointer");
        };
    };

    class bad_access : public std::exception {
    public:
        bad_access() {
            print_err("btree bad access");
        };
    };

    class bad_transaction : public std::exception {
    public:
        bad_transaction() {
            print_err("btree bad transaction");
        };
    };
    class total_leaf_error : public std::exception {
    public:
        total_leaf_error() {
            print_err("btree bad format");
        };
    };

#if 0
    class idle_processor {
    public:
        virtual void idle_time() = 0;
    };

    typedef std::vector<idle_processor *> _idle_processors; //, sta::tracker<idle_processor*>
#endif
    //extern _idle_processors idle_processors;
    //extern bool memory_low_state;
    //extern bool memory_mark_state;

    /// mini pointer for portable iterator references
    struct mini_pointer {

        storage::stream_address w;
    };


    /// an iterator intializer pair
    typedef std::pair<mini_pointer, unsigned short> initializer_pair;

    template<typename InterpolatedKeyType>
    struct interpolator {
#if 0
        inline bool encoded(bool) const {
            return false;
        }

        inline bool encoded_values(bool) const {
            return false;
        }

        inline bool attached_values() const {
            return false;
        }

        inline void encode(nst::buffer_type::iterator &, const _KeyType *, nst::u16) const {

        }

        inline void decode(const nst::buffer_type &, nst::buffer_type::const_iterator &, _KeyType *, nst::u16) const {
        }

        template<typename _AnyValue>
        inline void
        decode_values(const nst::buffer_type &, nst::buffer_type::const_iterator &, _AnyValue *, nst::u16) const {
        }

        template<typename _AnyValue>
        inline void encode_values(nst::buffer_type &, nst::buffer_type::iterator &, const _AnyValue *, nst::u16) const {
        }

        template<typename _AnyValue>
        inline size_t encoded_values_size(const _AnyValue *, nst::u16) const {
            return 0;
        }

        nst::i32 encoded_size(const _KeyType *, nst::u16) {
            return 0;
        }
#endif
        /// interpolation functions can be an something like linear interpolation
        inline bool can(const InterpolatedKeyType &, const InterpolatedKeyType &, nst::i32) const {
            return false;
        }
        /// prototype only
        /*nst::i32 interpolate(const _KeyType &, const _KeyType &, const _KeyType &, nst::i32 ) const {
            return 0;
        }*/
    };


    struct def_p_traits /// persist traits
    {

        typedef storage::stream_address stream_address;

        typedef unsigned long relative_address;

    };

    /** Generates default traits for a B+ tree used as a set. It estimates surface and
	* interior node sizes by assuming a cache line size of config::bytes_per_page bytes. */
    struct btree_traits {
        enum {
            bytes_per_page = 4096, /// this isnt currently used but could be
            max_scan = 0,
            keys_per_page = 72, ///depends mostly on average data sizee
            max_release = 8
        };
    };

    template<typename _Key, typename _PersistTraits>
    struct btree_default_set_traits {


        /// If true, the tree will self verify it's invariants after each insert()
        /// or erase(). The header must have been compiled with BTREE_DEBUG defined.
        static const bool selfverify = false;

        /// If true, the tree will print out debug information and a tree dump
        /// during insert() or erase() operation. The header must have been
        /// compiled with BTREE_DEBUG defined and key_type must be std::ostream
        /// printable.
        static const bool debug = false;

        /// persistence related addressing
        typedef def_p_traits persist_traits;

        typedef _Key key_type;

        /// Number of slots in each surface of the tree. Estimated so that each node
        /// has a size of about btree_traits::bytes_per_page bytes.
        static const nst::i32 surfaces = btree_traits::keys_per_page; //max_const( 8l, btree_traits::bytes_per_page / (sizeof(key_proxy)) );

        /// Number of slots in each interior node of the tree. Estimated so that each node
        /// has a size of about btree_traits::bytes_per_page bytes.
        static const nst::i32 interiorslots = btree_traits::keys_per_page; //max_const( 8l, btree_traits::bytes_per_page / (sizeof(key_proxy) + sizeof(void*)) );

        ///
        /// the max scan value for the hybrid lower bound that allows bigger pages
        static const nst::i32 max_scan = btree_traits::max_scan;

        /// Number of cached keys
        static const nst::i32 max_release = btree_traits::max_release;

    };

    /** Generates default traits for a B+ tree used as a map. It estimates surface and
	* interior node sizes by assuming a cache line size of btree_traits::bytes_per_page bytes. */
    template<typename _Key, typename _Data, typename _PersistTraits>
    struct bt_def_map_t /// default map traits
    {

        /// If true, the tree will self verify it's invariants after each insert()
        /// or erase(). The header must have been compiled with BTREE_DEBUG defined.
        static const bool selfverify = false;

        /// If true, the tree will print out debug information and a tree dump
        /// during insert() or erase() operation. The header must have been
        /// compiled with BTREE_DEBUG defined and key_type must be std::ostream
        /// printable.
        static const bool debug = false;

        /// traits defining persistence mechanisms and types

        typedef _PersistTraits persist_traits;

        typedef _Key key_type;

        typedef _Data data_type;

        typedef _Data mapped_type;

        /// Number of slots in each surface of the tree. A page has a size of about btree_traits::bytes_per_page bytes.

        static const nst::i32 surfaces = btree_traits::keys_per_page;//max_const( 8l, (btree_traits::bytes_per_page) / (sizeof(key_proxy) + sizeof(data_proxy)) ); //

        /// Number of slots in each interior node of the tree. a Page has a size of about btree_traits::bytes_per_page bytes.

        static const nst::i32 interiorslots = btree_traits::keys_per_page;//max_const( 8l,  (btree_traits::bytes_per_page*btree_traits::interior_mul) / (sizeof(key_proxy) + sizeof(void*)) );//
        ///
        /// the max scan value for the hybrid lower bound that allows bigger pages
        static const nst::i32 max_scan = btree_traits::max_scan;

        ///
        /// the max page releases during aggressive cleanup
        static const nst::i32 max_release = btree_traits::max_release;

    };

    enum states_ {
        initial = 1,
        unloaded,
        loaded,
        created,
        changed
    };
    typedef short states;

    static inline const char *get_state_name(states s) {
        thread_local std::string r;
        const char *sn = "";
        switch (s) {
            case initial:
                sn = "initial";
                break;
            case unloaded:
                sn = "unloaded";
                break;
            case loaded:
                sn = "loaded";
                break;
            case created:
                sn = "created";
                break;
            case changed:
                sn = "changed";
                break;
            default:;
        }
        r = sn;
        return r.c_str();
    }

    struct node_ref {
        mutable short s;

        /// the storage address for reference
        nst::stream_address address;

        void set_address_(nst::stream_address address) {
            (*this).address = address;
        }

        nst::stream_address get_address() const {
            return (*this).address;
        }

        node_ref() : s(initial), address(0) /// , a_refs(0)refs(0), orphaned(false),
        {
        }

        void set_state(states s) const {
            this->s = s;
        }

    };


    /// as a test the null ref was actually defined as a real address
    /// static node_ref null_ref;
    /// static node_ref * NULL_REF = &null_ref;

    /**
	* The base implementation of a stored B+ tree. Pages can exist in encoded or
	* decoded states. Insert is optimized using the stl::sort which is a hybrid
	* sort
	*
	* This class is specialized into btree_set, btree_multiset, btree_map and
	* btree_multimap using default template parameters and facade functions.
	*/



#define NULL_REF nullptr


    template<typename _Key, typename _Data, typename _Storage,
            typename _Value = std::pair<_Key, _Data>,
            typename _Compare = std::less<_Key>,
            typename _Iterpolator = persist::interpolator<_Key>,
            typename _Traits = bt_def_map_t<_Key, _Data, def_p_traits>,
            bool _Duplicates = false,
            typename _Alloc = std::allocator<_Value> >
    class btree {
    public:
        // *** Template Parameter Types

        /// Fifth template parameter: Traits object used to define more parameters
        /// of the B+ tree
        typedef _Traits traits;

        /// First template parameter: The key type of the B+ tree. This is stored
        /// in interior nodes and leaves
        typedef typename traits::key_type key_type;

        /// Second template parameter: The data type associated with each
        /// key. Stored in the B+ tree's leaves
        typedef _Data data_type;

        /// Third template parameter: Composition pair of key and data types, this
        /// is required by the STL standard. The B+ tree does not store key and
        /// data together. If value_type == key_type then the B+ tree implements a
        /// set.
        typedef _Value value_type;

        /// Fourth template parameter: Key comparison function object
        /// NOTE: this comparator is on the 'default type' of the proxy
        typedef _Compare key_compare;

        /// Fourth template parameter: interpolator if applicable
        typedef _Iterpolator key_interpolator;

        /// Fifth template parameter: Allow duplicate keys in the B+ tree. Used to
        /// implement multiset and multimap.
        static const bool allow_duplicates = _Duplicates;

        /// Seventh template parameter: STL allocator for tree nodes
        typedef _Alloc allocator_type;

        /// the persistent or not storage type
        typedef _Storage storage_type;

        /// the persistent buffer type for encoding pages
        typedef storage::buffer_type buffer_type;

        typedef typename _Traits::persist_traits persist_traits;

        typedef typename persist_traits::stream_address stream_address;
        enum {
            ROOT_PAGE = 1,
            TREE_SIZE = 2,
            PAGE_LIST_HEAD = 3,
            PAGE_LIST_TAIL = 4,
            LAST_PAGE_SIZE = 5,
            LEAF_NODES = 6,
            INTERIOR_NODES = 7
        };
    public:
        // *** Constructed Types

        /// Typedef of our own type
        typedef btree<key_type, data_type, storage_type, value_type, key_compare,
                key_interpolator, traits, allow_duplicates, allocator_type> btree_self;

        /// Size type used to count keys
        typedef size_t size_type;

        /// The pair of key_type and data_type, this may be different from value_type.
        typedef std::pair<key_type, data_type> pair_type;

        /// the automatic node reference type
        //typedef std::shared_ptr<node_ref> node_ref_ptr;


    public:
        // *** Static Constant Options and Values of the B+ Tree

        /// Base B+ tree parameter: The number of key/data slots in each surface
        static const unsigned short surfaceslotmax = traits::surfaces;

        /// Base B+ tree parameter: The number of key slots in each interior node,
        /// this can differ from slots in each surface.
        static const unsigned short interiorslotmax = traits::interiorslots;

        /// Computed B+ tree parameter: The minimum number of key/data slots used
        /// in a surface. If fewer slots are used, the surface will be merged or slots
        /// shifted from it's siblings.return
        static const unsigned short minsurfaces = (surfaceslotmax / 2);

        /// Computed B+ tree parameter: The minimum number of key slots used
        /// in an interior node. If fewer slots are used, the interior node will be
        /// merged or slots shifted from it's siblings.
        static const unsigned short mininteriorslots = (interiorslotmax / 2);

        /// Debug parameter: Enables expensive and thorough checking of the B+ tree
        /// invariants after each insert/erase operation.
        bool selfverify = traits::selfverify;

        /// Debug parameter: Prints out lots of debug information about how the
        /// algorithms change the tree. Requires the header file to be compiled
        /// with BTREE_DEBUG and the key type must be std::ostream printable.
        static const bool debug = traits::debug;

        /// parameter to control aggressive page release for the improvement of memory use
        static const nst::i32 max_release = traits::max_release;
        /// forward decl.

        struct tree_stats;

    private:
        // *** Node Classes for In-Memory and Stored Nodes

        /// proxy and interceptor class for reference counted pointers
        /// and automatic loading
        template<typename _Ptr>
        class base_proxy {
        protected:
            stream_address w;
            btree *context;
        public:
            bool check_zero_w = false;
            _Ptr ptr;

            stream_address get_w() const{
                return this->w;
            }
            
            void make_mini(mini_pointer &m) const {

                m.w = (*this).get_w();
            }

            inline void set_where(stream_address w) {

                (*this).w = w;

            }

            inline stream_address get_where() const {
                return (*this).w;
            }

        public:

            inline states get_state() const {
                if ((*this).ptr != NULL_REF) {
                    short s = (*this).ptr->s;

                    return s;
                }
                return w ? unloaded : initial;
            }

            inline bool is_changed() {
                return get_state() == changed;
            }
#ifdef DBG_PRINT
            inline void debug_state(states s) {

                if (nst::storage_debugging) {

                    print_dbg("setting state on", this->w,"to",get_state_name(s));
                }

            }
#else
            inline void debug_state(states) {

                if (nst::storage_debugging) {

                    print_dbg("setting state on", this->w,"to",get_state_name(s));
                }

            }
#endif
            inline void set_state(states s) {

                if ((*this).ptr != NULL_REF) {
                    if (s > 8) {
                        print_err("setting bad state");
                    }
                    debug_state(s);

                    (*this).ptr->s = s;
                }
            }

            void set_loaded() {
                set_state(loaded);
            }

            inline bool has_context() const {
                return (*this).context != NULL;
            }


            inline btree *get_context() const {
                return context;
            }

            storage_type *get_storage() const {
                return get_context()->get_storage();
            }

            inline bool is_loaded() const {
                return (*this).ptr != NULL_REF;
            }

            inline void set_context(btree *context) {
                //if ((*this).context == context) return;
                //if ((*this).context != NULL && context != NULL) {
                //print_err("suspicious context transfer");
                //throw bad_pointer();
                //}
                (*this).context = context;
            }

            explicit base_proxy() 
            :   w(0), context(NULL), ptr(NULL_REF) {

            }

            explicit base_proxy(const _Ptr &ptr)
            :   w(0), context(NULL), ptr(ptr) {

            }

            explicit base_proxy(std::nullptr_t)
                    :   w(0), context(NULL), ptr(nullptr) {

            }
            explicit base_proxy(const _Ptr &ptr, stream_address w)
            :   w(w), context(NULL), ptr(ptr) {

            }
#if 0
            ~base_proxy() {
            }
#endif
            inline bool not_equal(const base_proxy &left) const {

                return left.w != (*this).w;

            }

            inline bool equals(const base_proxy &left) const {

                return left.w == (*this).w;

            }

        };

        template<typename _Loaded>
        class pointer_proxy : public base_proxy<typename _Loaded::base_type::shared_ptr> {
        public:

            typedef base_proxy<typename _Loaded::base_type::shared_ptr> base_type;
            typedef base_proxy<typename _Loaded::base_type::shared_ptr> super;
        private:


            inline void clock_synch() {
                //if((*this).ptr)
                //	static_cast<_Loaded*>((*this).ptr)->cc = ++persist::cgen;
            }


        public:
            typedef _Loaded *loaded_ptr;
            typedef loaded_ptr &loaded_ptr_ref;
        public:
            inline void realize(const mini_pointer &m, btree *context) {
                (*this).set_where(m.w);
                (*this).context = context;
                
                if (context == NULL && (*this).get_w()) {
                    print_err("[this] is NULL");
                    throw bad_pointer();
                }
            }



            /// called to set the state and members correctly when member ptr is marked as created
            /// requires non zero w (wHere it gets stored) parameter

            bool create(btree *context, stream_address w) {

                this->set_where(w);
                this->set_context(context);
                auto storage = context->get_storage();
                bool result = false;
                if (!w) {
                    if (storage->is_readonly()) {

                        print_err("cannot allocate new page in readonly mode");
                        throw bad_transaction();
                    }
                    storage->allocate(super::w, persist::storage::create);
                    storage->complete();
                    this->get_context()->change_create(this->ptr, this->get_where());
                    result = true;
                }
                (*this).set_state(created);
                return result;


            }

            /// load the persist proxy and set its state to changed
            void change_before() {

                //print_dbg("trying to change %lld", (nst::lld) this->w);
                load();

                auto prev_state = (*this).get_state();
                switch ((*this).get_state()) {
                    case created:
                    case changed:
                        break;
                    default:
                        this->get_context()->change(this->ptr, this->get_where());
                        (*this).set_state(changed);

                }
#if 1
                if (nst::storage_debugging) {
                    if (!this->get_context()->is_changed(this->get_where())) {

                        print_err(
                                "modification status on node",this->w,"is not set (ERROR) prev state is",get_state_name(prev_state),"current state is"
                                , get_state_name((*this).get_state()));
                    }
                }
#endif
            }

            void change() {
                if (version_reload && (*this).ptr != NULL_REF && (*this).is_invalid()) {
                    /*print_err(
                            "Change should be called without an invalid member as this could mean that the operation preceding it is undone");
                            */
                    throw bad_access();
                }
                change_before();
            }

            void _save(btree &context) {

                switch ((*this).get_state()) {
                    case created:
                    case changed:

                        /// exit with a valid state
                        context.save(*this);

                        break;
                    case loaded:
                    case initial:
                    case unloaded:
                        break;
                }

            }

            void save(btree &context) {
                _save(context);

            }


            void unlink() {
                node *n = (*this).rget();
                if (n != NULL_REF)
                    n->update_links();
            }

            /// if the state is set to loaded and a valid wHere is set the proxy will change state to unloaded
            void unload_only() {
                auto s = (*this).get_state();
                if ((s == loaded || s == created || s == changed) && this->w) {
                    BTREE_ASSERT((*this).ptr != NULL_REF);

                    (*this).ptr = NULL_REF;

                }
                if (is_loaded()) {
                    print_err("page still loaded");
                }
            }

            void unload_ptr() {
                this->ptr = NULL_REF;
            }

            void unload(bool release = true, bool write_data = true) {
                if (write_data)
                    save(*this->get_context());
                if (release) {
                    unload_only();
                }
                if (is_loaded()) {
                    print_err("page still loaded");
                }
            }


            /// removes held references without saving dirty data and resets state
            void clear() {
                if (this->w && (*this).ptr != NULL_REF) {

                    (*this).ptr = NULL_REF;

                }
                this->set_state(initial);
                this->set_where(0);
            }

        private:

        public:

            void refresh() {
                reload_this(this);
            }

            void discard(btree &b) {
                (*this).context = &b;
                if ((*this).ptr != NULL_REF && (*this).get_state() == loaded) {

                    rget()->update_links();

                    unload_only();

                }

            }

            void flush(btree &b) {
                (*this).context = &b;
                (*this).save(b);

            }

            /// test the ptr next and preceding members to establish that they are the same pointer as the ptr member itself
            /// used for analyzing correctness emperically
            void test_btree() const {
                if ((*this).has_context() && super::w) {
                    if (super::ptr) {
                        stream_address sa = super::w;
                        const _Loaded *o = static_cast<const _Loaded *>((*this).get_context()->get_loaded(super::w));
                        const _Loaded *r = static_cast<const _Loaded *>(super::ptr);
                        if (o && r != o) {
                            print_err("different pointer to same resource");
                            throw bad_access();
                        }
                    }
                }
            }

            /// used to hide things from compiler optimizers which may inline to aggresively
            NO_INLINE void load_this(pointer_proxy *p) {//
                null_check();
                /// this is hidden from the MSVC inline optimizer which seems to be overactive
                if (this->has_context()) {
                    (*p) = ((*p).get_context()->load(((super *) p)->get_w()));
                } else {
                    print_err("no context supplied");
                    throw bad_access();
                }

            }
#if 0
            /// used to hide things from compiler optimizers which may inline to aggresively
            NO_INLINE void refresh_this(pointer_proxy *p) {//

                if (!version_reload) return;
                null_check();
                if (this->has_context()) {
                    (*p).get_context()->refresh(p->w);
                } else {
                    print_err("no context supplied");
                    throw bad_access();
                }

            }
#endif
            NO_INLINE void reload_this(pointer_proxy *p) {//
                if (!version_reload) return;
                null_check();
                /// this is hidden from the MSVC inline optimizer which seems to be overactive
                if (this->has_context()) {

                    (*p).get_context()->load(((super *) p)->w, rget());

                } else {
                    print_err("no context supplied");
                    throw bad_access();
                }

            }


            bool next_check() const {

                return true;
            }
            
            void null_check() const {
#if 0
                if (selfverify) {
                    if (this->get_where() == 0 && (*this).ptr != NULL_REF) {
                        print_err("invalid address");
                        throw bad_pointer();
                    }
                }
#endif
            }

            /// determines if the page should be loaded loads it and change state to loaded.
            /// The initial state can be any state
            void load() {

                null_check();

                if ((*this).ptr == NULL_REF) {
                    if (this->w) {
                        /// (*this) = (*this).get_context()->load(super::w);
                        /// replaced by
                        load_this(this);

                    }
                }
#if 0
                if ((*this).is_invalid()) {
                    refresh_this(this);
                }
#endif

            }

            /// const version of above function - un-consting it for eveel hackery
            FORCE_INLINE void load() const {
                null_check();
                pointer_proxy *p = const_cast<pointer_proxy *>(this);
                p->load();
            }

            const loaded_ptr shared() const {
                return static_cast<loaded_ptr>(this->ptr);
            }

            loaded_ptr shared() {
                return static_cast<loaded_ptr>(this->ptr);
            }

            /// lets the pointer go relinquishing any states or members held
            // ** Contructors
            pointer_proxy(const std::nullptr_t) : super(NULL_REF){

            }

            pointer_proxy(const typename _Loaded::shared_ptr &ptr)
            : super(ptr) {

            }

            /// constructs using given state and referencing the pointer if its not NULL_REF
            pointer_proxy(typename _Loaded::shared_ptr ptr, stream_address w, states s)
                    : super(ptr, w, s) {

            }

            /// installs pointer with clocksynch for LRU evictionset_address_change
            pointer_proxy(const pointer_proxy<_Loaded> &left) {
                *this = left;
                clock_synch();
            }

            pointer_proxy(const base_type &left) {
                *this = left;
                clock_synch();
            }


            pointer_proxy()
                    : super(NULL_REF) {

            }
#if 0
            ~pointer_proxy() {

            }
#endif
            pointer_proxy &operator=(const base_type &left) {
                if (this != &left) {

                    /// dont back propagate a context
                    if (left.has_context()) {
                        this->set_context(left.get_context());
                    }

                    if (this->ptr != left.ptr) {
                        this->ptr = left.ptr;
                    }
                    this->set_where(left.get_w());
                }
                return *this;
            }

            pointer_proxy &operator=(const pointer_proxy &left) {

                if (this != &left) {

                    /// dont back propagate a context
                    if (left.has_context()) {
                        this->set_context(left.get_context());
                    }

                    if (this->ptr != left.ptr) {
                        this->ptr = left.ptr;
                    }
                    this->set_where(left.get_w());
                }
                return *this;
            }
            pointer_proxy &operator=(const std::shared_ptr<_Loaded> &left) {

                if (this->ptr != left) {
                    this->ptr = left;
                }
                if (NULL_REF == left) {
                    (*this).ptr = NULL_REF;
                    this->set_where(0);
                }
                return *this;
            }
            pointer_proxy &operator=(const std::nullptr_t ) {

                (*this).ptr = NULL_REF;
                this->set_where(0);

                return *this;
            }
#if 0
            pointer_proxy &operator=(const _Loaded *left) {

                if (this->ptr != left) {
                    this->ptr = (_Loaded *) left;
                }
                if (NULL_REF == left) {
                    (*this).ptr = NULL_REF;
                    this->set_where(0);
                }
                return *this;
            }
#endif
            /// in-equality defined by virtual stream address
            /// to speed up equality tests each referenced
            /// instance has a virtual address preventing any
            /// extra and possibly invalid comparisons
            /// this also means that the virtual address is
            /// allocated instantiation time to ensure uniqueness
            inline bool operator!=(const pointer_proxy &left) const {

                return this->not_equal(left);
            }

            inline bool operator!=(const super &left) const {

                return this->not_equal(left);
            }

            /// equality defined by virtual stream address

            inline bool operator==(const super &left) const {

                return this->equals(left);
            }

            inline bool operator==(const pointer_proxy &left) const {

                return this->equals(left);
            }

            bool operator!=(const std::nullptr_t n) const {
                return !(this->ptr == n && this->w == 0);
            }

            bool operator==(const std::nullptr_t n) const {
                return this->ptr == n && this->w == 0;
            }

            /// return true if the member is not set
            bool empty() const {
                if ((*this).ptr != NULL_REF && this->w == 0) {
                    print_err("loaded pointer has no representation");
                }
                return (*this).ptr == NULL_REF;
            }
            /// return the raw member ptr without loading
            /// just the non const version

            inline loaded_ptr rget() {

                loaded_ptr r = static_cast<loaded_ptr>(this->ptr.get());
                //r->check_deleted();
                return r;
            }

            /// return the raw member ptr without loading

            inline const loaded_ptr rget() const {
                const loaded_ptr r = static_cast<const loaded_ptr>(this->ptr.get());
                //r->check_deleted();
                return r;
            }

            /// cast to its current type shared form
            loaded_ptr cast_shared() {
                return static_cast<loaded_ptr>((*this).ptr);
            }

            bool is_valid() const {
               
                /*if (this->has_context())
                     return this->get_context()->is_valid(rget(), this->w);*/
                 return true;
            }

            /// is this node transactionally invalid

            bool is_invalid() const {
                return !is_valid();
                //if (this->has_context())
                //    return this->get_context()->is_invalid(shared(), this->w);
                //return false;
            }
            /// 'natural' ref operator returns the pointer with loading

            inline loaded_ptr operator->() {

                if ((*this).ptr == NULL_REF) {
                    load();
                }
                //next_check();
                //rget()->check_deleted();

                return static_cast<loaded_ptr>(this->ptr.get());
            }

            /// 'natural' deref operator returns the pointer with loading

            _Loaded &operator*() {
                load();
                next_check();
                return *rget();
            }

            /// 'natural' ref operator returns the pointer with loading - through const violation

            inline const loaded_ptr operator->() const {
                load();

                return rget();
            }

            /// 'natural' deref operator returns the pointer with loading - through const violation

            const _Loaded &operator*() const {
                next_check();
                return *rget();
            }

            /// the 'safe' pointer getter
            inline const _Loaded *get() const {
                load();
                return rget();
            }

            /// the 'safe' pointer getter
            inline _Loaded *get() {
                load();
                return rget();
            }

            bool is_loaded() const {
                return (*this).ptr != NULL_REF;
            }
        };

        /// The header structure of each node in-memory. This structure is extended
        /// by interior_node or surface_node.
        /// TODO: convert node into 2 level mini b-tree to optimize CPU cache
        /// effects on large keys AND more importantly increase the page size
        /// without sacrificing random insert and read performance. An increased page size
        /// will result in improved LZ like compression on the storage end. will also
        /// result in a flatter b-tree with less page pointers resulting in less
        /// allocations and fragmentation hopefully
        /// Note: it seems on newer intel architecures the l2 <-> l1 bandwith is
        /// very high so the optimization may not be relevant
        struct node : public node_ref {
        public:
            typedef node base_type;
            /// ++11 shared pointer for thread safe memory management
            typedef std::shared_ptr<node> shared_ptr;
            /// raw pointer types
            typedef node *raw_ptr;
            typedef const raw_ptr const_raw_ptr;

        public:
            storage::i32 is_deleted;

            //ptrdiff_t refs;

        private:

            /// occupants: since all pages are the same size - its like a block of flats
            /// where the occupant count varies over time the size stays the same

            storage::u16 occupants = 0;

            /// the transaction version of this node
            nst::version_type version;

        protected:
            /// the persistence context
            btree *context = nullptr;

            /// the pages last transaction that it was checked on
            storage::u64 transaction = 0;

            /// changes
            size_t changes = 0;

            /// cached last found position for hierarchichal optimization
            mutable storage::u16 last_found;

            void check_node() const {
#if 0
                if (occupants > interiorslotmax + 1) {
                    print_err("page is probably corrupt");
                    throw bad_format();
                }
#endif
            }

            void set_address_change(const nst::stream_address &address) {
                change();
                node_ref::set_address_(address);
            }

        public:
            node() {
                this->context = NULL_REF;
                is_deleted = 0;
                this->s = initial;
                last_found = 0;
                changes = 0;
                //refs = 0;
            }

            ~node() {
                is_deleted = 1;
            }

            void ref() {
                //refs++;
                //this->context->stats.iterators_away++;
            }

            void unref() {
#if 0
                if (this->is_deleted != 0) {
                    print_err("unref deleted page");
                }
                if (refs == 0) {
                    print_err("invalid reference count");
                }
                if (this->context == NULL_REF) {
                    print_err("invalid context");
                }
#endif
                //if(this->context->stats.iterators_away==0){
                //    print_err("invalid reference count");
                //}
                //refs--;
                //this->context->stats.iterators_away--;
            }
#if 0
            bool is_ref() const {
                return refs != 0;
            }
#endif
            void set_context(btree *context) {
                this->context = context;
            }

            btree *get_context() const {
                return this->context;
            }

            void check_deleted() const {
                // if (is_deleted != 0) {
                //     print_err("node is deleted");
                //     throw bad_access();
                //}
            }

            void inc_occupants() {
                ++occupants;

                check_node();
            }
            /// set transaction checked

            nst::u64 get_transaction() const {
                check_node();
                return (*this).transaction;
            }

            void set_transaction(nst::u64 transaction) {
                check_node();
                (*this).transaction = transaction;
            }

            /// get version

            const nst::version_type &get_version() const {

                return (*this).version;
            }

            /// set version

            void set_version(nst::version_type version) {
                check_node();
                (*this).version = version;
            }

            void check_int_occ() const {
#if 0
                if (isinteriornode() && occupants == 0) {
                    print_dbg("no occupants for this node?");
                }
#endif
            }

            void dec_occupants() {
                check_node();
                if (occupants > 0)
                    --occupants;
                check_int_occ();
            }

            /// return the key value pair count

            storage::u16 get_occupants() const {
                check_node();
                check_int_occ();
                return occupants;
            }

            /// set the key value pair count

            void set_occupants(storage::u16 o) {
                check_node();
                occupants = o;
                check_int_occ();
            }


            /// Level in the b-tree, if level == 0 -> surface node

            storage::u16 level;

            /// clock counter for lru a.k.a le roux

            //storage::u32 cc;

            /// Delayed initialisation of constructed node

            inline void initialize(btree *context, const unsigned short l) {
                this->context = context;
                level = l;
                occupants = 0;
                changes = 0;
                version = nst::version_type();
                transaction = 0;
                address = 0;

                set_context(context);

            }

            size_t get_changes() const {
                return changes;
            }

            void change() {
                ++this->changes;
            }
            /// True if this is a surface node

            inline bool issurfacenode() const {
                check_node();
                return (level == 0);
            }

            inline bool isinteriornode() const {
                check_node();
                return (level != 0);
            }

            /// persisted reference type

            typedef pointer_proxy<node> ptr;

            inline bool key_lessequal(key_compare key_less, const key_type &a, const key_type &b) const {
                return !key_less(b, a);
            }

            inline bool key_less_(key_compare key_less, const key_type &a, const key_type &b) const {
                return key_less(a, b);
            }

            inline bool key_equal(key_compare key_less, const key_type &a, const key_type &b) const {
                return !key_less(a, b) && !key_less(b, a);
            }

            /// multiple search type lower bound template function
            /// performs a lower bound mapping using a couple of techniques simultaneously
            /// h is always 1 larger than the search range
            template<typename key_compare, typename node_type>
            inline int find_lower_range(key_compare key_less, const node_type *node,
                                  const key_type &key, int l, int h) const {

                /// optimization which likes linear data and does not burn random perf.
                if (h > 0 && key_lessequal(key_less, node->get_key(h-1), key )) {
                    l = h - 1;
                } else if (key_lessequal(key_less, key, node->get_key(l))) {
                    h = l;
                }
                int m;
                while (l < h) {
                    m = (l + h) >> 1;
                    /// less branchy version of binary search
                    if (key_lessequal(key_less, key, node->get_key(m))) {
                        h = m;
                    } else {
                        l = m + 1;
                    }
                }
                return l;
            }

            template<typename key_compare, typename node_type>
            inline int find_lower(key_compare key_less, const node_type *node,
                                  const key_type &key) const {
                //check_node();
                int o = get_occupants();
                if (o == 0)
                    return o;
                return find_lower_range(key_less, node, key, 0, o);
                //dbg_print("lower bound on page %lld = %d of %d keys",(nst::lld)this->get_address(),l,(int)this->get_occupants());

            }

            void destroy_orphan() {
                if (this->level == 0) {
                    return static_cast<surface_node *>(this)->destroy_orphan();
                }
            }

            void update_links() {
                if (this->level == 0) {
                    return static_cast<surface_node *>(this)->update_links();
                }
            }

            void validate_surface_links() const {
#if 0
                if (selfverify && this != nullptr) {
                    if (this->level == 0) {
                        return static_cast<const surface_node *>(this)->validate_surface_links();
                    }
                }
#endif
            }

            bool next_check() const {
#if 0
                if (selfverify && this != nullptr) {
                    if (this->level == 0) {
                        return static_cast<const surface_node *>(this)->next_check();
                    }
                }

                return false;
#else
                return true;
#endif
            }

        };

        /// a decoded interior node in-memory. Contains keys and
        /// pointers to other nodes but no data. These pointers
        /// may refer to encoded or decoded nodes.

        struct interior_node : public node {
            typedef node base_type;

            /// Define a related allocator for the interior_node structs.
            /// typedef typename _Alloc::template rebind<interior_node>::other alloc_type;
            typedef typename std::allocator_traits<_Alloc>::template rebind_alloc<interior_node> alloc_type;

            /// ++11 shared pointer for threadsafe nmemory management
            typedef std::shared_ptr<interior_node> shared_ptr;

            /// persisted reference type providing unobtrusive page management
            typedef pointer_proxy<interior_node> ptr;


            /// return a child id
            typename node::ptr &get_childid(int at) {
                return this->childid[at];
            }

            const
            typename node::ptr &get_childid(int at) const {
                return this->childid[at];
            }

            void set_childid(int at, const typename node::ptr &child) {
                this->childid[at] = child;

            }
        private:
            key_type mid_key;
        public:
            void setup_internal_keys(){
                auto o = node::get_occupants();
                mid_key = get_key(o/2);
            }

        private:

            /// Keys of children or data pointers
            key_type _keys[interiorslotmax];

            /// Pointers to sub trees
            typename node::ptr childid[interiorslotmax + 1];

            /// keys accessor
            key_type *_D_keys() {
                return &_keys[0];
            }

            const
            key_type *_D_keys() const {
                return &_keys[0];
            }

        public:
            virtual ~interior_node() {
                //this->context->stats.interiornodes--;
            }

            const
            key_type &get_key(int at) const {
                return _keys[at];
            }


            void set_key(int at, const key_type &key) {
                _keys[at] = key;
            }

            /// Set variables to initial values
            inline void initialize(btree *context, const unsigned short l) {
                node::initialize(context, l);
            }

            /// True if the node's slots are full
            inline bool isfull() const {
                return (node::get_occupants() == interiorslotmax);
            }

            /// True if few used entries, less than half full
            inline bool isfew() const {
                return (node::get_occupants() <= mininteriorslots);
            }

            /// True if node has too few entries
            inline bool isunderflow() const {
                return (node::get_occupants() < mininteriorslots);
            }

            template<typename key_compare>
            inline int find_lower(key_compare key_less, const key_type &key) const {
#if 0
                int l2a;
                if (key_less( key, mid_key )) {
                    l2a = node::find_lower_range(key_less, this, key, 0, node::get_occupants()/2);
                }else
                    l2a = node::find_lower_range(key_less, this, key, node::get_occupants()/2, node::get_occupants());
 /*
                const bool verify = false;
                if(verify){
                    auto l2c = node::find_lower_range(key_less, this, key, 0, node::get_occupants());
                    if(l2a != l2c)
                        print_err("occ.",node::get_occupants(),"l2a",l2a,"l2c",l2c,);
                    return l2c;
                }
*/
                return l2a;
#else
                //return std::distance(&_keys[0], std::lower_bound(&_keys[0], &_keys[node::get_occupants()], key, key_less));
                return node::find_lower_range(key_less, this, key, 0, node::get_occupants());
#endif
            }

            /// decodes a page from given storage and buffer and puts it in slots
            /// the buffer type is expected to be some sort of vector although no strict
            /// checking is performedinterior_node
            template<typename key_interpolator>
            void load(btree *context, stream_address address, nst::version_type version, storage_type &storage,
                     const buffer_type &buffer, size_t /*bsize*/, key_interpolator /*interp*/
                    ) {
                print_dbg("loading interior node at page", address);
                using namespace persist::storage;
                buffer_type::const_iterator reader = buffer.begin();
                //node::check_deleted();
                (*this).address = address;
                nst::i16 occupants = (nst::i16) leb128::read_signed(reader);
                if (occupants <= 0 || occupants > interiorslotmax + 1) {
                    print_err("bad format: loading invalid interior node size",occupants);
                    throw bad_format();
                }
                (*this).set_occupants(occupants);
                (*this).level = leb128::read_signed(reader);
                if ((*this).level <= 0 || (*this).level > 128) {
                    print_err("bad format: loading invalid level for interior node",this->level);
                    throw bad_format();
                }
                (*this).set_version(version);
                for (u16 k = 0; k <= interiorslotmax; ++k) {
                    childid[k] = NULL_REF;
                }
                //node::check_deleted();
                for (u16 k = 0; k < (*this).get_occupants(); ++k) {
                    reader = storage.retrieve(buffer, reader, _keys[k]);
                }
                for (u16 k = 0; k <= (*this).get_occupants(); ++k) {
                    stream_address sa = (stream_address) leb128::read_signed64(reader, buffer.end());
                    childid[k].set_context(context);
                    childid[k].set_where(sa);
                }


                this->transaction = storage.current_transaction_order();
                if (this->transaction == 0) {
                    print_err("the transaction supplied is empty");
                    throw bad_transaction();
                }
                this->check_node();
                this->check_int_occ();
                setup_internal_keys();
            }

            /// encodes a node into storage page and resizes the output buffer accordingly
            /// TODO: check for any failure conditions particularly out of memory
            /// and throw an exception

            void save(storage_type &storage, buffer_type &buffer) const {
                this->check_node();
                if (this->get_occupants() < 0 || this->get_occupants() > interiorslotmax + 1) {
                    print_err("bad format: saving invalid interior node size", this->get_occupants());
                    throw bad_format();
                }
                if ((*this).level <= 0 || (*this).level > 128) {
                    print_err("bad format: saving invalid level for interior node");
                    throw bad_format();
                }
                using namespace persist::storage;
                u32 storage_use = leb128::signed_size((*this).get_occupants()) + leb128::signed_size((*this).level);
                for (u16 k = 0; k < (*this).get_occupants(); ++k) {
                    storage_use += storage.store_size(get_key(k));
                    storage_use += leb128::signed_size(childid[k].get_where());
                }
                storage_use += leb128::signed_size(childid[(*this).get_occupants()].get_where());

                buffer.resize(storage_use);
                auto limit = buffer.end();
                buffer_type::iterator writer = buffer.begin();

                writer = leb128::write_signed(writer, (*this).get_occupants());
                writer = leb128::write_signed(writer, (*this).level);

                for (u16 k = 0; k < (*this).get_occupants(); ++k) {
                    writer = storage.store(writer, limit, get_key(k));
                }
                ptrdiff_t d = writer - buffer.begin();
                for (u16 k = 0; k <= (*this).get_occupants(); ++k) {
                    writer = leb128::write_signed(writer, childid[k].get_where());
                }

                d = writer - buffer.begin();
                buffer.resize(d); /// TODO: use swap
            }

            void clear_surfaces() {
                using namespace persist::storage;
                /// removes any remaining references to surfaces if available
                if (this->level == 1) {

                    for (u16 c = 0; c <= interiorslotmax; ++c) {
                        childid[c].unload_ptr(); //.discard(*(this->context));
                    }
                }
            }

            void clear_references() {
                this->check_node();
                using namespace persist::storage;
                /// removes any remaining references to surfaces
                for (u16 c = this->get_occupants() + 1; c <= interiorslotmax; ++c) {
                    childid[c].clear();
                }
            }

            void set_address(stream_address address) {
                node_ref::set_address_(address);
            }

            bool next_check() const {
                return false;
            }
        };

        /// Extended structure of a surface node in memory. Contains pairs of keys and
        /// data items. Key and data slots are kept in separate arrays, because the
        /// key array is traversed very often compared to accessing the data items.

        struct surface_node : public node {

            typedef node base_type;
            typedef std::shared_ptr<surface_node> shared_ptr;

            /// smart persistence pointer for reasonably unobtrusive page storage management
            typedef pointer_proxy<surface_node> ptr;

            /// raw reference pointer types
            typedef surface_node *raw_ptr;
            typedef const raw_ptr const_raw_ptr;

            /// Define a related allocator for the surface_node structs.
            /// typedef typename _Alloc::template rebind<surface_node>::other alloc_type;
            typedef typename std::allocator_traits<_Alloc>::template rebind_alloc<surface_node> alloc_type;
            /// Double linked list pointers to traverse the leaves

            typename surface_node::ptr preceding;

            std::allocator<key_type> key_allocator;
            std::allocator<data_type> data_allocator;

            mutable std::mutex excl;
        protected:
            /// Double linked list pointers to traverse the leaves
            typename surface_node::ptr next;

            /// data can be kept in encoded format for less memory use
            //buffer_type						attached;
            
            /// for partial construction on page - it improves creation speed on complex key types like std::string
            /// this is only used when the key and or value type has a constructor
            std::bitset<surfaceslotmax> constructed;
            
            /// permutation map
            nst::u8 permutations[surfaceslotmax];

            /// allocation marker
            nst::u16 allocated;
            /// count keys hashed
            nst::u16 hashed;
            /// Keys of children or data pointers
#if BT_CONSTRUCTOR_OPT_
            nst::u8 _keys[surfaceslotmax*sizeof(key_type)];
#else
            key_type _keys[surfaceslotmax];
#endif
            /// Array of data
#if BT_CONSTRUCTOR_OPT_
            nst::u8 _values[surfaceslotmax*sizeof(data_type)];
#else
            data_type _values[surfaceslotmax];
#endif

            /// initialize a key at a specific position
            void init_key_allocation(int at) {
                if (permutations[at] == surfaceslotmax) {

                    if (allocated == surfaceslotmax) {
                        if (this->get_occupants() < allocated) {
                            for (nst::u16 p = this->get_occupants(); p < surfaceslotmax; ++p) {
                                permutations[p] = surfaceslotmax;
                            }
                            allocated = this->get_occupants();
                        } else {
                            //print_err("cannot allocate any more keys");
                            throw bad_format();
                        }
                    }
                    permutations[at] = allocated;
                    ++allocated;
                }
            }


        public:
            void destroy_orphan() {
#if 0
                if (this->is_deleted != 0) {
                    print_err("destroy deleted page");
                }
                
                if (this->refs == 0) {
                    this->is_deleted = 1;
                    typename surface_node::alloc_type surface_allocator;
                    this->deallocate_data(this->context);

                    print_dbg("destroying", this->get_address(),"in storage", this->context->get_storage()->get_name());
                    //surface_allocator.destroy(this);
                    //this->~surface_node();
                    //surface_allocator.deallocate(this, 1);

                }
#endif
            }

            void deallocate_data(btree *context) {
                if (this->context == nullptr) {
                    if (this->hashed > 0)
                        print_err("cannot have hashed keys without context");
                    return;
                }
                if (this->hashed > 0) {
                    for (int at = 0; at < surfaceslotmax; ++at) {
                        nst::u16 al = permutations[at];
                        if (al != surfaceslotmax) {
                            context->erase_hash(get_key(al));
                        }
                        permutations[at] = surfaceslotmax;
                    }
                }
            }

            void unload_links() {
                this->preceding.unload_ptr();
                this->next.unload_ptr();
            }

            ~surface_node() {
#if BT_CONSTRUCTOR_OPT_
                for(nst::u32 a = 0; a < surfaceslotmax; ++a){
                    destruct_pair(a);
                }
#endif
                --btree_totl_surfaces;
                print_dbg("destroy surface node", this->get_address());
            }

            void check_next() const {
                return;
                if (this->get_address() > 0 && next.get_where() == this->get_address()) {
                    print_err("set bad next");
                    throw bad_access();
                }
            }

            void validate_surface_links() {
                if (selfverify) {
                    if (this != nullptr && (*this).issurfacenode()) {
                        surface_node *c = this;
                        if (c->get_next().ptr != NULL_REF) {

                            const surface_node *n = static_cast<surface_node *>(c->get_next().rget());
                            if (n->preceding.get_where() != n->get_where()) {
                                print_err("The node has invalid preceding pointer");
                                throw bad_access();
                            }
                        }
                        if (c->preceding.ptr != NULL_REF) {
                            surface_node *p = static_cast<surface_node *>(c->preceding.rget());
                            if (p->get_next().get_where() != (*this).get_where()) {
                                print_err("The node has invalid next pointer");
                                throw bad_access();
                            }
                        }
                    }
                }
            }

            void update_links() {
                if (this->issurfacenode()) {
                    if (preceding.is_loaded()) {
                        preceding.rget()->unload_next();
                        preceding.unload(true, false);
                    }
                    if (get_next().is_loaded()) {
                        const
                        surface_node *n = const_cast<typename btree::surface_node *>(get_next().rget());
                        const_cast<typename btree::surface_node *>(n)->preceding.unload(true, false);
                        const_cast<typename btree::surface_node *>(n)->unload_next();
                    }
                } else {
                    print_err("surface node reports its not a surface node");
                    throw bad_access();
                }

            }

            bool next_check() const {
                if (selfverify) {
                    if
                            (this != nullptr
                             && this->get_address() > 0
                             && (*this).get_where() > 0
                            ) {
                        if
                        (   this->get_address() == this->get_next().get_where()
                        ) {
                            print_err("next check failed");
                            throw bad_access();

                        }

                    }
                }
                return true;
            }

            buffer_type &get_attached() {
                return (*this).attached;
            }

            const buffer_type &get_attached() const {
                return (*this).attached;
            }

            void transfer_attached(buffer_type &attached) {
                (*this).attached.swap(attached);
            }

            void set_next(const ptr &next) {

                (*this).next = next;
                check_next();
            }

            void set_next_preceding(const ptr &np) {
                (*this).next->preceding = np;
                check_next();
            }

            void set_next_context(btree *context) {
                this->next.set_context(context);
                check_next();
            }

            void unload_next() {

                this->next.unload(true, false);
                check_next();
            }

            void set_address_change(stream_address address) {
                node::set_address_change(address);
                check_next();
            }

            void set_address(stream_address address) {

                node_ref::set_address_(address);
                check_next();
            }

            const ptr &get_next() const {
                check_next();
                return (*this).next;
            }

            ptr &get_next()  {
                check_next();
                return (*this).next;
            }

            void change_next() {
                (*this).next.change_before();
                check_next();
            }


            template<typename key_compare>
            inline int find_lower(key_compare key_less, const key_type &key) const {
                return node::find_lower(key_less, this, key);

            }

            
            
            void move_kv(nst::i32 at, key_type &k, data_type &v) {
                auto &p = permutations[at];
                if (p == surfaceslotmax) {
#if 0
                    if (allocated == surfaceslotmax) {
                        print_err("cannot allocate any more keys");
                        throw bad_format();
                    }
#endif
                    p = allocated++;
                }
                this->change();

#if BT_CONSTRUCTOR_OPT_
                construct_pair(p);
                *cast_entry<key_type>(p,_keys) = std::move(k);
                *cast_entry<data_type>(p,_values) = std::move(v);
#else
               _keys[p] = std::move(k);
               _values[p] = std::move(v);
#endif
            }

            void set_key(nst::i32 at, const key_type &key) {
                init_key_allocation(at);
#if BT_CONSTRUCTOR_OPT_
                *cast_entry<key_type>(construct_pair(permutations[at]), _keys) = key;
#else
                _keys[permutations[at]] = key;
#endif
                this->change();
            }

            inline key_type &get_key(nst::i32 at) {
                init_key_allocation(at);
                this->change();
#if BT_CONSTRUCTOR_OPT_
                return *reinterpret_cast<key_type*>(&_keys[construct_pair(permutations[at])*sizeof(key_type)]);
#else
                return _keys[permutations[at]];
#endif
            }
#if BT_CONSTRUCTOR_OPT_
            inline void destruct_pair(nst::u32 at) {

                if(!constructed[at]) return;
                key_type *k = cast_entry<key_type>(at, _keys);
                data_type *d = cast_entry<data_type>(at, _values);
                //key_allocator.destroy(k);
                //data_allocator.destroy(d);
                k->~key_type();
                d->~data_type();
                constructed[at] = false;

            }

            inline nst::u32 construct_pair(nst::u32 at) const {

                return const_cast<surface_node*>(this)->construct_pair(at);

            }
            inline nst::u32 construct_pair(nst::u32 at){

                if(constructed[at]) return at;
                key_type *k = cast_entry<key_type>(at, _keys);
                data_type *d = cast_entry<data_type>(at, _values);
                new (k) key_type ();
                //key_allocator.construct(k);
                //data_allocator.construct(d);
                new (d) data_type ();
                constructed[at] = true;

                return at;
            }
            template<typename _ET>
            _ET* cast_entry(nst::u32 at, nst::u8 * from){
                return reinterpret_cast<_ET*>(&from[at * sizeof(_ET)]);
            }
            template<typename _ET>
            const _ET* cast_entry(nst::u32 at, const nst::u8 * from) const {
                return reinterpret_cast<const _ET*>(&from[at * sizeof(_ET)]);
            }
#endif
            inline const key_type &get_key(int at) const {
                if (permutations[at] == surfaceslotmax) {
                    //print_err("invalid surface page key access");
                    throw bad_access();
                }

                auto pat = permutations[at];

#if BT_CONSTRUCTOR_OPT_
                construct_pair(pat);
                return *cast_entry<key_type>(pat, _keys);
#else
                return  _keys[pat];
#endif
            }

            typedef std::unordered_map<stream_address, typename node::shared_ptr> _AddressedNodes;

            inline const data_type &get_value(int at) const {
                if (permutations[at] == surfaceslotmax) {
                    print_err("invalid surface page value access");
                    throw bad_access();
                }

                auto p = permutations[at];
#if BT_CONSTRUCTOR_OPT_
                return *cast_entry<data_type>(construct_pair(p), _values);
#else
                return _values[p];
#endif
            }

            inline data_type &get_value(int at) {
                init_key_allocation(at);
                this->change();
                auto p = permutations[at];
#if BT_CONSTRUCTOR_OPT_
                return *cast_entry<data_type>(construct_pair(p),_values);
#else
                return _values[p];
#endif
            }

            /// update keys at a position
            void update(int at, const key_type &key, const data_type &value) {
                get_key(at) = key;
                get_value(at) = value;
            }

            /// insert a key and value pair at
            void insert_(int at, const key_type &key, const data_type &value) {
                BTREE_ASSERT(this->get_occupants() < surfaceslotmax);
                int j = this->get_occupants();
                nst::u16 t = permutations[j]; /// because its a resource that might be overwritten
                for (; j > at;) {
                    permutations[j] = permutations[j - 1];
                    --j;
                }
                permutations[at] = t;
                get_key(at) = key;
                get_value(at) = value;
                //print_dbg("insert key at",at,"on page",this->get_address());
                //this->context->add_hash(this,at);
                //++hashed;
                (*this).inc_occupants();
            }

            void copy(int to, const surface_node *other, int start, int count) {
                for (unsigned int slot = start; slot < count; ++slot) {
                    unsigned int ni = to + slot - start;
                    get_key(ni) = other->get_key(slot);
                    get_value(ni) = other->get_value(slot);
                }
                /// indicates change for copy on write semantics
                this->set_occupants(count);
            }

            void erase_d(int slot) {
                if (this->context != nullptr && hashed > 0) {
                    this->context->erase_hash(get_key(slot));
                }
                if (this->get_occupants() > 0) {
                    print_dbg("erase", slot,"of", this->get_occupants(),"on page", slot, this->get_address());
                    int e = this->get_occupants() - 1;
                    int saved = permutations[slot];
                    for (int i = slot; i < e; i++) {
                        permutations[i] = permutations[i + 1];

                    }

                    permutations[e] = saved;

                    this->change();
                    this->dec_occupants();
                }

            }

            /// default constructor anyone
            surface_node() {
                ++btree_totl_surfaces;
                (*this).hashed = 0;
            }

            /// Set variables to initial values
            inline void initialize(btree *context) {
                node::initialize(context, 0);
                (*this).context = context;
                (*this).transaction = 0;
                (*this).preceding = next = NULL_REF;
                (*this).allocated = 0;
                (*this).hashed = 0;
                for (nst::u16 p = 0; p < surfaceslotmax; ++p) {
                    permutations[p] = surfaceslotmax;
                }

            }

            /// True if the node's slots are full

            inline bool isfull() const {
                return (node::get_occupants() == surfaceslotmax);
            }

            /// True if few used entries, less or equal to half full

            inline bool isfew() const {
                return (node::get_occupants() <= minsurfaces);
            }

            /// True if node has too few entries

            inline bool isunderflow() const {
                return (node::get_occupants() < minsurfaces);
            }


            /// assignment
            surface_node &operator=(const surface_node &right) = delete;
            /// sorts the page if it hasn't been

            template<typename key_compare>
            struct key_data {
                key_compare l;
                key_type key;
                data_type value;

                inline bool operator<(const key_data &left) const {
                    return l(key, left.key);
                }
            };

            /// sort the keys and move values accordingly

            template<typename key_compare>
            inline bool key_equal(key_compare key_less, const key_type &a, const key_type &b) const {
                return !key_less(a, b) && !key_less(b, a);
            }



            /// decodes a page into a exterior node using the provided buffer and storage instance/context
            /// TODO: throw an exception if checks fail

            void load
                    (btree *loading_context, stream_address address, nst::version_type version, storage_type &storage,
                     buffer_type &buffer, size_t bsize, key_interpolator /*interp*/
                    ) {
                print_dbg("loading surface node at page", address);
                using namespace persist::storage;
                /// TODO: implement direct encoding some time
                (*this).address = address;
                /// size_t bs = buffer.size();
                buffer_type::const_iterator reader = buffer.begin();
                nst::i16 occupants = (nst::i16) leb128::read_signed(reader);
                if (occupants < 0 || occupants > surfaceslotmax) {
                    print_err("bad format: invalid node/page size");
                    throw bad_format();
                }
                (*this).set_occupants(occupants);
                (*this).level = leb128::read_signed(reader);
                if ((*this).level != 0) {
                    print_err("bad format: invalid level for surface node");
                    throw bad_format();
                }
                //nst::i32 encoded_key_size = leb128::read_signed(reader);
                //nst::i32 encoded_value_size = leb128::read_signed(reader);
                (*this).set_version(version);
                (*this).next = NULL_REF;
                (*this).preceding = NULL_REF;
                stream_address sa = leb128::read_signed(reader);
                /// TODO: this is a problem for multi-threading - we might have to make context a thread_local somehow
                (*this).preceding.set_context(loading_context);
                (*this).preceding.set_where(sa);
                sa = leb128::read_signed(reader);
                if (sa == address) {
                    print_err("loading node with invalid next address");
                    throw bad_format();
                }
                /// TODO: this is a problem for multi-threading - we might have to make context a thread_local somehow
                (*this).next.set_context(loading_context);
                (*this).next.set_where(sa);

                //if (encoded_key_size > 0) {
                //    print_err("cannot decode encoded pages or invalid format");
                //    throw bad_format();
                //}
               
                for (u16 k = 0; k < (*this).get_occupants(); ++k) {
                    key_type &key = get_key(k);
                    reader = storage.retrieve(buffer, reader, key);
                }
                (*this).hashed = 0;
                // if (encoded_value_size > 0) {
                //     print_err("cannot decode encoded pages or invalid format");
                //     throw bad_format();
                //}
                
                bool add_hash = (storage.is_readonly() && !(::persist::memory_low_state));
                for (u16 k = 0; k < (*this).get_occupants(); ++k) {
                    reader = storage.retrieve(buffer, reader, get_value(k));
                    if (add_hash) {
                        //++((*this).hashed);/// only add hash cache when readonly
                        //loading_context->add_hash(self, k);
                    }
                }

                size_t d = reader - buffer.begin();

                if (d != bsize) {
                    BTREE_ASSERT(d == buffer.size());
                    print_err("surface nodes of invalid size");
                    throw bad_format();
                }
                print_dbg("loading surface", this->get_address(), d,"bytes");
                this->transaction = storage.current_transaction_order();
                if (this->transaction == 0) {
                    print_err("the transaction supplied is empty");
                    throw bad_transaction();
                }

            }

            /// Encode a stored page from input buffer to node instance
            /// TODO: throw an exception if read iteration extends beyond
            /// buffer size

            void save(key_interpolator /*interp*/, storage_type &storage, buffer_type &buffer) const {
                using namespace persist::storage;
                //bool direct_encode = false;
                //nst::i32 encoded_key_size = 0; // (nst::i32)interp.encoded_size(_keys, (*this).get_occupants());
                //nst::i32 encoded_value_size = 0; // (nst::i32)interp.encoded_values_size(values(), (*this).get_occupants());
                //encoded_key_size = 0;
                //encoded_value_size = 0;

                ptrdiff_t storage_use = leb128::signed_size((*this).get_occupants());
                storage_use += leb128::signed_size((*this).level);
                //storage_use += leb128::signed_size(encoded_key_size);
                //storage_use += leb128::signed_size(encoded_value_size);
                storage_use += leb128::signed_size(preceding.get_where());
                storage_use += leb128::signed_size(next.get_where());

                storage_use += sizeof(key_type) * (*this).allocated;
                for (u16 k = 0; k < (*this).get_occupants(); ++k) {
                    storage_use += storage.store_size(get_key(k));
                }
                storage_use += sizeof(data_type) * (*this).allocated;
                for (u16 k = 0; k < (*this).get_occupants(); ++k) {
                    storage_use += storage.store_size(get_value(k));
                }
                buffer.resize(storage_use);
                if (buffer.size() != (size_t) storage_use) {
                    print_err("storage resize failed");
                    throw bad_pointer();
                }
                buffer_type::iterator writer = buffer.begin();
                writer = leb128::write_signed(writer, (*this).get_occupants());
                writer = leb128::write_signed(writer, (*this).level);
                //writer = leb128::write_signed(writer, encoded_key_size);
                //writer = leb128::write_signed(writer, encoded_value_size);
                writer = leb128::write_signed(writer, preceding.get_where());
                writer = leb128::write_signed(writer, next.get_where());
                
                auto limit = buffer.end();

                for (u16 k = 0; k < (*this).get_occupants(); ++k) {
                    writer = storage.store(writer, limit, get_key(k));
                }
                
                for (u16 k = 0; k < (*this).get_occupants(); ++k) {
                    writer = storage.store(writer, limit, get_value(k));
                }

                ptrdiff_t d = writer - buffer.begin();
                if (d > storage_use) {
                    BTREE_ASSERT(d <= storage_use);
                }
                print_dbg("saving surface", this->get_address(), d, "bytes");
                buffer.resize(d);
            }
        };

    private:
        struct cache_data {
            cache_data() : key(nullptr), value(nullptr), node(nullptr) {}

            cache_data(const cache_data &right)
                    : node(right.node), key(right.key), value(right.value) {}

            cache_data(const typename surface_node::shared_ptr &node, key_type *key, data_type *value)
                    : node(node), key(key), value(value) {

            }

            cache_data &operator=(const cache_data &right) {
                node = right.node;
                key = right.key;
                value = right.value;
                return *this;
            }

            ~cache_data() {

            }

            typename surface_node::raw_ptr node;
            key_type *key;
            data_type *value;
        };

        /// a fast lookup table to speedup finds in constant time
        typedef std::unordered_map<size_t, cache_data> _NodeHash; //, ::sta::tracker<_AllocatedSurfaceNode,::sta::bt_counter>

        mutable
        _NodeHash key_lookup; /// must be the last table to destroy because no sp's

        // ***Smart pointer based dynamic loading
        /// each node is uniquely identified by a 'virtual'
        /// address even empty ones.
        /// Each address is mapped to physical media
        /// i.e. disk, ram, pigeons etc. Allows the storage
        /// and information structure optimization domains to
        /// be decoupled.
        /// TODO: the memory used by these is not counted
        typedef std::unordered_map<stream_address, typename node::shared_ptr> _AddressedNodes;
        typedef std::unordered_map<stream_address, typename surface_node::shared_ptr> _AddressedSurfaceNodes;
        typedef std::unordered_map<stream_address, typename interior_node::shared_ptr> _AddressedInteriorNodes;
        //typedef std::unordered_map<stream_address, node*> _AddressedNodes;


        typedef std::pair<const surface_node *, nst::u16> _HashKey;
        typedef std::pair<stream_address, ::persist::storage::version_type> _AddressPair;
        typedef std::pair<stream_address, typename node::shared_ptr> _AllocatedNode;
        typedef std::vector<_AllocatedNode> _AllocatedNodes;

        /// provides register for currently loaded/decoded nodes
        /// used to prevent reinstantiation of existing nodes
        /// therefore providing consistency to updates to any
        /// node

        _AddressedNodes nodes_loaded;
        _AddressedInteriorNodes interiors_loaded;
        _AddressedSurfaceNodes surfaces_loaded;
        _AddressedNodes modified;

        void erase_hash(const key_type &key) {
            size_t h = hash_val(key);
            if (h) {

                key_lookup.erase(h);
            }
        }

        ///	returns NULL if a node with given storage address is not currently
        /// loaded. otherwise returns the currently loaded node

        typename node::const_raw_ptr get_loaded(stream_address w) const {
            auto i = nodes_loaded.find(w);
            if (i != nodes_loaded.end())
                return (*i).second;
            else
                return NULL_REF;
            return nullptr;
        }

        _AddressPair make_ap(stream_address w) {
            get_storage()->allocate(w, persist::storage::read);
            nst::version_type version = get_storage()->get_allocated_version();
            _AddressPair ap = std::make_pair(w, version);
            get_storage()->complete();
            return ap;
        }

        void change_create(const typename interior_node::shared_ptr &n, stream_address w) {
            print_dbg("changing interior node", w, n->get_occupants());
            n->set_address(w);
            //if (n->get_changes() == 0) {
                modified[w] = n;
            //}

        }

        bool is_changed(stream_address w) {
            return modified.count(w);
        }
        /// called when a page is changed

        void change(const typename interior_node::shared_ptr &n, stream_address w) {
            //print_dbg("changing interior node", w, n->get_occupants());
            n->set_address(w);

            //if (!modified.count(w)) {
            //if(n->get_changes() == 0){
                modified[w] = n;
            //}

        }

        /// same for surface nodes

        void change(const typename surface_node::shared_ptr &n, stream_address w) {
            if (w == last_surface.get_where()) {
                stats.last_surface_size = last_surface->get_occupants();
            }
            print_dbg("changing surface node", w, n->get_occupants());
            n->set_address_change(w);
            //if(n->get_changes() == 0){
                modified[w] = n;
            //}

        }

        void change_create(const typename node::shared_ptr &n, stream_address w) {
            if (n->issurfacenode()) {
                change(std::static_pointer_cast<surface_node>(n), w);
            } else {
                change_create(std::static_pointer_cast<interior_node>(n), w);
            }

        }
        /// change an abstract node

        void change(const typename node::shared_ptr &n, stream_address w) {
            if (n->issurfacenode()) {
                change(std::static_pointer_cast<surface_node>(n), w);
            } else {
                change(std::static_pointer_cast<interior_node>(n), w);
            }
        }

        /// called to direct the saving of a inferior node to storage through instance
        /// management routines in the b-tree

        void save(interior_node *n, stream_address &w) {

            if (get_storage()->is_readonly()) {
                print_err("trying to save resource to read only transaction");
                throw bad_transaction();
            }
            print_dbg("[B-TREE SAVE] i-node",w,"->",get_storage()->get_name(),"ver.",w,nst::tostring(get_storage()->get_version()));
            using namespace persist::storage;
            buffer_type &buffer = get_storage()->allocate(w, persist::storage::create);

            n->save(*get_storage(), buffer);

            if (lz4) {
                inplace_compress_lz4(buffer, temp_compress);
            } 
            //n->s = loaded;
            get_storage()->complete();
            n->set_state(loaded);
            stats.changes++;


        }

        /// called to direct the saving of a node to storage through instance
        /// management routines in the b-tree

        void save(typename interior_node::ptr &n) {
            stream_address w = n.get_where();
            save(n.rget(), w);
            n.set_where(w);

        }

        /// called to direct the saving of a surface node to storage through instance
        /// management routines in the b-tree
        buffer_type temp_compress;
        buffer_type create_buffer;

        void save(surface_node *n, stream_address &w) {
            if (storage == nullptr) {
                print_err("trying to write resource without context");
                throw bad_access();
            }
            if (get_storage()->is_readonly()) {
                print_err("trying to write resource to read only transaction");
                throw bad_transaction();

            }

            print_dbg("[B-TREE SAVE] s-node",w,"->",get_storage()->get_name(),"ver.",
                      nst::tostring(get_storage()->get_version()));
            using namespace persist::storage;


            if (n->get_address() > 0 && n->get_address() == n->get_next().get_where()) {
                print_err("saving node with recursive address");
                throw bad_access();
            }
            buffer_type full;
            create_buffer.clear();

            n->save(key_interpolator(), *get_storage(), create_buffer);

            if (lz4) {
                compress_lz4_fast(full, create_buffer);
            } else {
                full.swap(create_buffer);
            }
            n->s = loaded; /// no state change message on purpose
            buffer_type &allocated = get_storage()->allocate(w, persist::storage::create);
            allocated.swap(full);
            get_storage()->complete();
            stats.changes++;
            n->set_state(loaded);
        }

        /// called to direct the saving of a surface node to storage through instance
        /// management routines in the b-tree

        void save(typename surface_node::ptr &n) {
            stream_address w = n.get_where();
            save(n.rget(), w);
            n.set_where(w);
        }

        /// called to route the saving of a interior or exterior node to storage through instance
        /// management routines in the b-tree

        void save(typename node::ptr &n) {
            if (get_storage()->is_readonly()) {
                return;
            }
            if (n->issurfacenode()) {
                typename surface_node::ptr s = n;
                save(s);
                n = s;
            } else {
                typename interior_node::ptr s = n;
                save(s);
                n = s;
            }

        }

        void save(const typename node::shared_ptr &n, stream_address w) {
            if (n == NULL_REF) {
                print_err("attempting to save NULL");
                throw bad_format();
                return;
            }
            if (get_storage()->is_readonly()) {
                return;
            }
            if (n->issurfacenode()) {
                typename surface_node::ptr s = std::static_pointer_cast<surface_node>(n);
                s.set_context(this);
                s.set_where(w);
                save(s);

            } else {
                typename interior_node::ptr s = std::static_pointer_cast<interior_node>(n);
                s.set_context(this);
                s.set_where(w);
                save(s);
            }

        }

        /// called to route the loading of a interior or exterior node to storage through instance
        /// management routines in the b-tree
        buffer_type load_buffer;
        buffer_type temp_buffer;

        bool is_valid(const node *page) const {
            return is_valid(page, page->get_address());
        }

        bool is_valid(const node *page, stream_address /*w*/) const {

            if (!version_reload) return true;
            if (page != NULL_REF) {
                if (!page->get_version().is_nonzero()) return true;
                if (get_storage()->current_transaction_order() < page->get_transaction()) {
                    // a transaction cannot load a page that has been committed after this transaction started
                    // but it could also be a nalformed page ?
                    throw bad_format();
                }
                bool tx_ok = get_storage()->current_transaction_order() == page->get_transaction();

                return tx_ok;
            }
            return false;
        }

        bool is_invalid(const typename node::shared_ptr &page, stream_address w) const {

            return !is_valid(page, w);
        }

        /// refresh preallocated surface node
        typename node::ptr refresh(stream_address w) {

            return load(w);

        }


        /// check a bunch of preconditions on a surface node
        void surface_check(const surface_node *surface, stream_address w) const {
            if (surface && surface->get_address() && w != surface->get_address()) {

                const surface_node *surface_test1 = nullptr;
                const surface_node *surface_test2 = nullptr;

                typename _AddressedNodes::const_iterator s = nodes_loaded.find(surface->get_address());
                if (s != nodes_loaded.end()) {
                    surface_test1 = static_cast<const surface_node *>((*s).second);
                }
                s = nodes_loaded.find(w);
                if (s != nodes_loaded.end()) {
                    surface_test2 = static_cast<const surface_node *>((*s).second);
                    if (surface_test2 == surface_test1) {
                        print_err("multiple addresses pointing to same node");
                        throw bad_format();
                    }
                }

                s = nodes_loaded.find(surface->get_address());
                if (s != nodes_loaded.end()) {
                    surface_test1 = static_cast<const surface_node *>((*s).second);
                }
                s = nodes_loaded.find(w);
                if (s != nodes_loaded.end()) {
                    surface_test2 = static_cast<const surface_node *>((*s).second);
                    if (surface_test2 == surface_test1) {
                        print_err("multiple addresses pointing to same node (2)");
                        throw bad_format();
                    }
                }
            }
        }

        template<typename _Loaded>
        void load_node(_Loaded &s, stream_address w, const nst::version_type &version, buffer_type &buffer,
                       size_t load_size) {
            s.set_where(w);
            s->load(this, w, version, *(get_storage()), buffer, load_size, key_interpolator());
            s.set_state(loaded);
            s.set_where(w);

        }

        ///TODO: let it be known I hate this function but I'm to stoopid to rewrite it
        typename node::ptr load(stream_address w) {
            using namespace persist::storage;
            auto l = nodes_loaded.find(w);
            if (l != nodes_loaded.end()) {
                typename node::ptr s = l->second;
                print_dbg("loading node",w,"from memory");
                s.set_state(loaded);
                s.set_where(w);
                return s;
            }

            i32 level = 0;
            /// storage operation started
            buffer_type &dangling_buffer = get_storage()->allocate(w, persist::storage::read);
            if (get_storage()->is_end(dangling_buffer) || dangling_buffer.size() == 0) {
                print_err("bad allocation at %li in %s", (long int) w, get_storage()->get_name().c_str());

                BTREE_ASSERT(get_storage()->is_end(dangling_buffer) && dangling_buffer.size() > 0);
                throw bad_transaction();
            }

            nst::version_type version = get_storage()->get_allocated_version();
            
            size_t load_size = dangling_buffer.size();
            if (lz4) {
                load_size = r_decompress_lz4(load_buffer, dangling_buffer);
            } else {
                load_buffer = dangling_buffer;
            }
            get_storage()->complete();
            /// storage operation complete

            buffer_type::iterator reader = load_buffer.begin();
            leb128::read_signed(reader);
            level = leb128::read_signed(reader);

            if (level == 0) { // its a safaas
                typename surface_node::ptr s;
                s = allocate_surface(w);
                load_node(s, w, version, load_buffer, load_size);
                return s;
            } else {
                typename interior_node::ptr s;
                s = allocate_interior(level, w);
                load_node(s, w, version, load_buffer, load_size);
                return s;
            }


        }

        // *** Template Magic to Convert a pair or key/data types to a value_type

        /// For sets the second pair_type is an empty struct, so the value_type
        /// should only be the first.
        template<typename value_type, typename pair_type>
        struct btree_pair_to_value {
            /// Convert a fake pair type to just the first component
            inline value_type operator()(pair_type &p) const {
                return p.first;
            }

            /// Convert a fake pair type to just the first component
            inline value_type operator()(const pair_type &p) const {
                return p.first;
            }
        };

        /// For maps value_type is the same as the pair_type
        template<typename value_type>
        struct btree_pair_to_value<value_type, value_type> {
            /// Identity "convert" a real pair type to just the first component
            inline value_type operator()(pair_type &p) const {
                return p;
            }

            /// Identity "convert" a real pair type to just the first component
            inline value_type operator()(const pair_type &p) const {
                return p;
            }
        };

        /// Using template specialization select the correct converter used by the
        /// iterators
        typedef btree_pair_to_value<value_type, pair_type> pair_to_value_type;

    public:
        // *** Iterators and Reverse Iterators

        class iterator;

        class const_iterator;

        class reverse_iterator;

        class const_reverse_iterator;

        class iterator_base {
        public:
            typedef typename btree::surface_node::ptr surface_ptr;
            typedef unsigned short slot_type;

            /// Friendly to the const_iterator, so it may access the two data items directly.
            friend class iterator;

            /// Friendly to the const_iterator, so it may access the two data items directly.
            friend class const_iterator;

            /// Also friendly to the reverse_iterator, so it may access the two data items directly.
            friend class reverse_iterator;

            /// Also friendly to the const_reverse_iterator, so it may access the two data items directly.
            friend class const_reverse_iterator;

            /// Also friendly to the base btree class, because erase_iter() needs
            /// to read the currnode and current_slot values directly.
            friend class btree<key_type, data_type, storage_type, value_type, key_compare, key_interpolator, traits, allow_duplicates>;

        public:
            // *** Methods
            iterator_base() : check_point(0) {

            }

            explicit iterator_base(slot_type slot, nullptr_t)
                    : current_ptr(nullptr), check_point(0), current_slot(slot) {

                ref();
            };

            iterator_base(slot_type slot, const surface_ptr &currnode)
                    : current_ptr(currnode), check_point(0), current_slot(slot) {

                ref();
            };

            iterator_base(const iterator_base &other) : check_point(0) {
                this->assign(other);
            };

            iterator_base &operator=(const iterator_base &other) {
                this->assign(other);
                return *this;
            }

            ~iterator_base() {
                unref();

            };

            /**
             * reposition the iterator towards the end specified by finish
             * if finish does not exist then the iterator will
             * advance to the end of the tree
             * @tparam _Iterator
             * @param at Current key/data slot referenced
             * @param count
             * @param boundary must be the lower or upper most position
             * can be empty iterator if unknown, then the operation will
             * continue until the internal boundary is reached
             */
            template<typename _Iterator>
            void reposition
                    (_Iterator &/*at*/, long long count, const _Iterator &boundary = _Iterator()
                    ) {
                if (count > 0)
                    _advance(count, boundary);
                else
                    _retreat(-count, boundary);
            }


            /**
             * advances the iterator towards the end specified by finish
             * if finish does not exist then the iterator will
             * advance to the end of the tree
             * @tparam _Iterator
             * @param at Current key/data slot referenced
             * @param count
             * @param finish
             */
            template<typename _Iterator>
            void base_advance
                    (long long count, const _Iterator &finish
                    ) {
                _advance(count, finish);
            }

            /**
             * reverses the iterator up to start if start does not
             * exist within the tree the iterator will continue
             * to the begining of the tree
             * @tparam _Iterator
             * @param at Current key/data slot referenced
             * @param count
             * @param start
             */
            template<typename _Iterator>
            void base_retreat
                    (long long count, const _Iterator &start
                    ) {
                _retreat(count, start);
            }

            void ref() {
                if (current_ptr.is_loaded()) {
                    current_ptr.rget()->ref();
                }
            }

            void unref() {
                if (current_ptr.is_loaded()) {
                    auto surface = current_ptr.rget();
                    surface->unref();
                    //surface->destroy_orphan();
                    current_ptr = NULL_REF;
                }

            }

        private:
            // *** Members
            /// The currently referenced surface node of the tree
            surface_ptr current_ptr;
        protected:
            // *** Members
            /// Check points any changes
            size_t check_point;
            /// Current key/data slot referenced
            slot_type current_slot;

        private:
            // *** Methods
            /**
             * take the iterator forward by count
             * the steps taken are minimized
             * if count is negative no action is taken
             *
             */
            /**
             * take the iterator forward by count
             * the steps taken are minimized
             * if count is negative no action is taken
             * if the finish position is behind current
             * then current will continue to internal boundary
             * @param currnode
             * @param current_slot
             * @param count
             * @param finish
             */
            template<typename _Iterator>
            void _advance
                    (long long count, const _Iterator &finish
                    ) {

                if (get_current() == nullptr) return;
                // pre con: currnode is != nullptr
                long long remaining = count < 0 ? -count : count;
                // pre con: remaining >= 0
                if (finish.get_current() == get_current()) {
                    this->current_slot = std::min<long long>(current_slot + remaining, finish.current_slot);
                    return;
                }
                // pre con start.currnode != currnode
                for (;;) {
                    if (this->current_slot + remaining < get_current()->get_occupants()) {
                        this->current_slot += remaining;
                        break;
                    } else if (get_current()->get_next() != nullptr) {
                        auto context = get_current().get_context();
                        if (context) context->check_low_memory_state();
                        remaining -= (get_current()->get_occupants() - current_slot);
                        assign_current(get_current()->get_next());
                        this->current_slot = 0;
                        if (get_current() == finish.get_current()) {
                            this->current_slot = std::min<long long>(current_slot + remaining, finish.current_slot);
                            break;
                        }


                    } else {
                        this->current_slot = get_current()->get_occupants();
                        remaining = 0;
                        break;
                    }
                };
                // post con: remaining is 0
                // post con: currnode != nullptr
                check_point = get_current()->get_changes();
            }

            /**
             * let the iterator retreat from the parameter described by at
             * @tparam _Iterator
             * @param at
             * @param count
             * @param start
             */
            template<typename _Iterator>
            void _retreat
                    (long long count, const _Iterator &start
                    ) {
                if (get_current() == nullptr) return;
                // pre con: currnode is != nullptr
                long long remaining = count < 0 ? -count : count;
                // pre con: remaining >= 0
                if (start.get_current() == get_current()) {
                    this->current_slot = start.current_slot;
                    return;
                }
                // pre con start.currnode != currnode
                while (remaining > 0) {
                    if (this->current_slot > remaining) {
                        this->current_slot -= remaining;
                        remaining = 0;
                    } else if (get_current()->preceding != NULL_REF) {

                        // if (currnode.has_context()) currnode.get_context()->check_low_memory_state();
                        /// |0..n-1|==n subtract remaining before currnode change if current_slot has been set to
                        /// a value < currnode->get_occupants() - 1 then we will need to subtract that
                        remaining -= current_slot;
                        assign_current(get_current()->preceding);
                        if (get_current() == start.get_current()) {
                            this->current_slot = start.current_slot;
                            remaining = 0;
                            break;
                        }
                        this->current_slot = get_current()->get_occupants() - 1;
                    } else {/// reached external start
                        this->current_slot = get_current()->get_occupants();
                        remaining = 0;
                        break;
                    }
                }
                // post con: currnode is != nullptr and remaining == 0
                check_point = get_current()->get_changes();
            }

        protected:
            void update_check_point() {
                if (current_ptr.is_loaded()) {
                    check_point = get_current()->get_changes();
                } else {
                    check_point = 0;
                }
            }

            bool has_changed() {
                if (current_ptr.is_loaded()) {
                    auto changes = current_ptr->get_changes();
                    if (check_point != changes) {
                        check_point = changes;
                        return true;
                    } else {
                        return false;
                    }

                }

                return false;
            }

            void assign(const iterator_base &other) {

                if (&other != this) {
                    unref();
                    this->current_slot = other.current_slot;
                    current_ptr = other.current_ptr;
                    ref();
                }


            }

            void assign_current(const surface_ptr &other) {
                unref();
                current_ptr = other;
                ref();


            }

            surface_ptr &get_current() {
                if (current_ptr != NULL_REF && !current_ptr.is_loaded()) {
                    current_ptr->ref();
                }
                return current_ptr;
            }

            const surface_ptr &get_current() const {
                if (current_ptr != NULL_REF && !current_ptr.is_loaded()) {
                    current_ptr->ref();
                }
                return current_ptr;
            }

        };

        /// STL-like iterator object for B+ tree items. The iterator points to a
        /// specific slot number in a surface.
        class iterator : public iterator_base {
        public:
            // *** Types

            /// The key type of the btree. Returned by key().
            typedef typename btree::key_type key_type;

            /// The data type of the btree. Returned by data().
            typedef typename btree::data_type data_type;

            /// The value type of the btree. Returned by operator*().
            typedef typename btree::value_type value_type;

            /// The pair type of the btree.
            typedef typename btree::pair_type pair_type;

            /// Reference to the value_type. STL required.
            typedef value_type &reference;

            /// Pointer to the value_type. STL required.
            typedef value_type *pointer;

            /// STL-magic iterator category
            typedef std::bidirectional_iterator_tag iterator_category;

            /// STL-magic
            typedef ptrdiff_t difference_type;

            /// Our own type
            typedef iterator self;

            /// an iterator intializer pair
            typedef persist::initializer_pair initializer_pair;

        private:
            // *** Members


            //key_type * _keys;
            //data_type * _data;
            /// Friendly to the const_iterator, so it may access the two data items directly.
            friend class iterator_base;

            friend class const_iterator;

            /// Also friendly to the reverse_iterator, so it may access the two data items directly.
            friend class reverse_iterator;

            /// Also friendly to the const_reverse_iterator, so it may access the two data items directly.
            friend class const_reverse_iterator;

            /// Also friendly to the base btree class, because erase_iter() needs
            /// to read the currnode and current_slot values directly.
            friend class btree<key_type, data_type, storage_type, value_type, key_compare, key_interpolator, traits, allow_duplicates>;

            /// Evil! A temporary value_type to STL-correctly deliver operator* and
            /// operator->
            //  mutable value_type              temp_value;
        private:

            /// assign pointer caches
            void assign_pointers() {
                iterator_base::update_check_point();

                //_data = &currnode->values()[0];
                //_keys = &currnode->keys()[0];
            }

            void initialize_pointers() {
                //_data = NULL_REF;
                //_keys = NULL_REF;
            }

        public:
            // *** Methods

            /// Default-Constructor of a mutable iterator
            inline iterator()
                    : iterator_base(0, NULL_REF) //, _data(NULL_REF), _keys(NULL_REF)
            {}

            /// Initializing-Constructor of a mutable iterator
            /// TODO: this probably wont work anyway
            inline iterator(const btree */*source*/, typename btree::surface_node::ptr l, unsigned short s)
                    : iterator_base(s, l) {
                assign_pointers();
            }

            /// Initializing-Constructor-pair of a mutable iterator
            inline iterator(const initializer_pair &initializer)
                    : iterator_base(initializer.second, initializer.first) //, _data(NULL_REF), _keys(NULL_REF)
            {
                assign_pointers();
            }

            /// Copy-constructor from a reverse iterator
            inline iterator(const reverse_iterator &it)
                    : iterator_base(it) //, _data(NULL_REF), _keys(NULL_REF)
            {
                assign_pointers();
            }

            ~iterator() {}
            /// The next to operators have been comented out since they will cause
            /// problems in shared mode because they cannot 'render' logical
            /// keys(), (keys pointed to but  not  loaded) may be fixed with
            /// hackery but not likely

            /// Dereference the iterator, this is not a value_type& because key and
            /// value are not stored together
            /*inline reference operator*() const
			{
			temp_value = pair_to_value_type()( pair_type(currnode->keys()[current_slot],
			currnode->values()[current_slot]) );
			return temp_value;
			}*/

            /// Dereference the iterator. Do not use this if possible, use key()
            /// and data() instead. The B+ tree does not store key and data
            /// together.
            /*inline pointer operator->() constNULL_REF
			{
			temp_value = pair_to_value_type()( pair_type(currnode->keys()[current_slot],
			get_current()->values()[current_slot]) );
			return &temp_value;
			}*/

            /// Key of the current slot
            inline const key_type &key() const {
                //return _keys[current_slot];
                return this->get_current()->get_key(this->current_slot);
            }

            inline key_type &key() {
                //return _keys[current_slot];
                key_type &test = this->get_current()->get_key(this->current_slot);
                return test;
            }

            key_type *operator->() {
                return &key();
            }

            key_type &operator*() {
                return key();
            }

            bool is_valid() const {
                return this->current_slot < this->get_current()->get_occupants();
            }

            void validate() {
                iterator_base::update_check_point();
            }

            /// Writable reference to the current data object
            inline data_type &data() {
                //return _data[current_slot];
                return this->get_current()->get_value(this->current_slot);
            }

            inline const data_type &data() const {
                //return _data[current_slot];index
                return this->get_current()->get_value(this->current_slot);
            }

            inline data_type &value() {
                return data();
            }

            inline const data_type &value() const {
                return data();
            }

            /// return true if the iterator is valid
            inline bool loadable() const {
                return this->get_current().has_context();
            }

            /// iterator construction pair
            inline initializer_pair construction() const {
                mini_pointer m;
                this->get_current().make_mini(m);
                return std::make_pair(m, this->current_slot);
            }

            inline bool has_changed() {
                return iterator_base::has_changed();
            }

            /// refresh finction to load the latest version of the current node
            /// usually only used for debugging
            void refresh() {
                if (this->get_current() != NULL_REF) {
                    this->get_current().refresh();
                }
            }

            /// iterator constructing pair
            template<typename _MapType>
            inline bool from_initializer(_MapType &context, const initializer_pair &init) {
                this->get_current().realize(init.first, &context);
                if (this->current_slot < this->get_current()->get_occupants()) {
                    this->current_slot = init.second;
                    assign_pointers();
                    return true;
                }
                return false;
            }

            inline bool from_initializer(const initializer_pair &init) {
                this->get_current().realize(init.first, this->get_current().get_context());
                if (this->current_slot < this->get_current()->get_occupants()) {
                    this->current_slot = init.second;
                    assign_pointers();
                    return true;
                }
                return false;

            }

            inline iterator &operator=(const initializer_pair &init) {
                this->get_current().realize(init.first, this->get_current().get_context());
                this->current_slot = init.second;
                assign_pointers();
                return *this;
            }

            inline iterator &operator=(const iterator &other) {
                iterator_base::assign(other);
                this->current_slot = other.current_slot;
                iterator_base::check_point = other.iterator_base::check_point;
                assign_pointers();
                return *this;
            }

            /**
             * advances/retreats iterator by count steps
             * (optimized)
             * @param count if negative advances
             * @return thisreposition
             */
            inline self &operator-=(const long long count) {
                if (count > 0)
                    iterator_base::base_retreat(count, iterator());
                else
                    iterator_base::base_advance(-count, iterator());
                return *this;
            }

            /**
             * advances/retreats iterator by count steps
             * (optimized)
             * @param count if negative retreats
             * @return this
             */
            inline self &operator+=(const long long count) {
                if (count >= 0)
                    iterator_base::base_advance(count, iterator());
                else
                    iterator_base::base_retreat(-count, iterator());
                return *this;
            }

            void advance(unsigned long long count, const iterator &upper) {
                iterator_base::base_advance(count, upper);
            }

            /// returns true if the iterator is invalid
            inline bool invalid() const {
                return (!this->get_current().has_context() || this->get_current().get_where() == 0);
            }

            /// clear context references
            inline void clear() {
                this->current_slot = 0;
                this->get_current().set_context(NULL);
                initialize_pointers();
            }

            /// returns true if the iterator is valid
            inline bool valid() const {
                return (this->get_current().has_context() && this->get_current().get_where() != 0);
            }

            /// mechanism to quickly count the keys between this and another iterator
            /// providing the other iterator is at a higher position
            inline persist::storage::u64 count(const self &to) const {

                typename btree::surface_node::ptr last_node = to.get_current();
                typename btree::surface_node::ptr first_node = this->get_current();
                typename persist::storage::u64 total = 0ull;
                auto context = first_node.get_context();
                while (first_node != NULL_REF && first_node != last_node) {

                    total += first_node->get_occupants();

                    first_node = first_node->get_next();
                    //if (context) context->check_low_memory_state();

                }

                total += to.current_slot;
                total -= this->current_slot;
                print_dbg("total returned",total,"2cs",to.current_slot,"this cs", this->current_slot);
                return total;
            }

            /// ++Prefix advance the iterator to the next slot
            inline self &operator++() {

                if (this->current_slot + 1 < this->get_current()->get_occupants()) {
                    ++this->current_slot;
                } else if (this->get_current()->get_next() != NULL_REF) {
                    /// TODO:
                    /// add a iterator only function here to immediately let go of iterated pages
                    /// when memory is at a premium (which it usually is)
                    ///


                    auto context = this->get_current().get_context();

                    if (context) context->check_low_memory_state();
                    this->assign_current(this->get_current()->get_next());
                    this->current_slot = 0;
                    assign_pointers();
                } else {
                    // this is end()

                    this->current_slot = this->get_current()->get_occupants();
                }

                return *this;
            }


            /// Postfix++ advance the iterator to the get_next() slot
            inline self operator++(int) {
                self tmp = *this;   // copy ourselves


                if (this->current_slot + 1 < this->get_current()->get_occupants()) {
                    ++this->current_slot;
                } else if (this->get_current()->get_next() != NULL_REF) {
                    //if (this->get_current().has_context()) this->get_current().get_context()->check_low_memory_state();

                    this->assign_current(this->get_current()->get_next());

                    this->current_slot = 0;
                    assign_pointers();
                } else {
                    // this is end()

                    this->current_slot = this->get_current()->get_occupants();
                }

                return tmp;
            }

            /// return the proxy context
            const void *context() const {
                return this->get_current().get_context();
            }

            /// --Prefix backstep the iterator to the last slot
            inline self &operator--() {

                if (this->current_slot > 0) {
                    --this->current_slot;
                } else if (this->get_current()->preceding != NULL_REF) {
                    //if (get_current().has_context()) get_current().get_context()->check_low_memory_state();

                    this->assign_current(this->get_current()->preceding);

                    this->current_slot = this->get_current()->get_occupants() - 1;
                    assign_pointers();
                } else {
                    assign_pointers();
                    // this is begin()
                    assign_pointers();
                    this->current_slot = 0;
                }

                return *this;
            }

            /// Postfix-- backstep the iterator to the last slot
            inline self operator--(int) {

                self tmp = *this;   // copy ourselves

                if (this->current_slot > 0) {
                    --this->current_slot;
                } else if (this->get_current()->preceding != NULL_REF) {
                    //if (get_current().has_context())get_current().get_context()->check_low_memory_state();

                    this->assign_current(this->get_current()->preceding);
                    this->current_slot = this->get_current()->get_occupants() - 1;
                    assign_pointers();
                } else {
                    this->current_slot = 0;

                }

                return tmp;
            }

            /// Equality of iterators
            inline bool operator==(const self &x) const {
                return (x.get_current() == this->get_current()) && (x.current_slot == this->current_slot);
            }

            /// Inequality of iterators
            inline bool operator!=(const self &x) const {
                return (x.get_current() != this->get_current()) || (x.current_slot != this->current_slot);
            }
        };

        /// STL-like read-only iterator object for B+ tree items. The iterator
        /// points to a specific slot number in a surface.
        class const_iterator : public iterator_base {
        public:
            // *** Types

            /// The key type of the btree. Returned by key().
            typedef typename btree::key_type key_type;

            /// The data type of the btree. Returned by data().
            typedef typename btree::data_type data_type;

            /// The value type of the btree. Returned by operator*().
            typedef typename btree::value_type value_type;

            /// The pair type of the btree.
            typedef typename btree::pair_type pair_type;

            /// Reference to the value_type. STL required.
            typedef const value_type &reference;

            /// Pointer to the value_type. STL required.
            typedef const value_type *pointer;

            /// STL-magic iterator category
            typedef std::bidirectional_iterator_tag iterator_category;

            /// STL-magic
            typedef ptrdiff_t difference_type;

            /// Our own type
            typedef const_iterator self;

        private:
            // *** Members

            friend class iterator_base;

            /// Friendly to the reverse_const_iterator, so it may access the two data items directly
            friend class const_reverse_iterator;

            /// hamsterveel! A temporary value_type to STL-correctly deliver operator* and
            /// operator->
            mutable value_type temp_value;

            /// check point
            mutable size_t check_point;


        public:
            // *** Methods
            void set_check_point() const {
                if (this->get_current() != NULL_REF) {
                    check_point = this->get_current()->get_changes();
                }
            }

            void validate() {
                iterator_base::update_check_point();
            }

            inline bool has_changed() {
                if (this->get_current() != NULL_REF) {
                    return check_point != this->get_current()->get_changes();
                }
                return false;
            }

            bool is_valid() const {
                return this->current_slot < this->get_current()->get_occupants();
            }

            ~const_iterator() {

            }

            /// Default-Constructor of a const iterator
            inline const_iterator()
                    : iterator_base(NULL_REF) {}

            /// Initializing-Constructor of a const iterator
            /// TODO: should be tested
            inline const_iterator(const btree */*source*/, const typename btree::surface_node::ptr l, unsigned short s)
                    : iterator_base(s, l) {
                set_check_point();
            }

            /// Copy-constructor from a mutable iterator
            inline const_iterator(const iterator &it)
                    : iterator_base(it) {
                set_check_point();
            }

            /// Copy-constructor from a mutable reverse iterator
            explicit inline const_iterator(const reverse_iterator &it)
                    : iterator_base(it) {
                set_check_point();
            }

            /// Copy-constructor from a const reverse iterator
            inline const_iterator(const const_reverse_iterator &it)
                    : iterator_base(it) {
                set_check_point();
            }

            /// Dereference the iterator. Do not use this if possible, use key()
            /// and data() instead. The B+ tree does not stored key and data
            /// together.
            inline reference operator*() const {
                temp_value = pair_to_value_type()(pair_type(this->get_current()->get_key(this->current_slot),
                                                            this->get_current()->get_value(this->current_slot)));
                return temp_value;
            }

            /// Dereference the iterator. Do not use this if possible, use key()
            /// and data() instead. The B+ tree does not stored key and data
            /// together.
            inline pointer operator->() const {
                temp_value = pair_to_value_type()(pair_type(this->get_current()->get_key(this->current_slot),
                                                            this->get_current()->get_value(this->current_slot)));
                return &temp_value;
            }

            /// Key of the current slot
            inline const key_type &key() const {
                return this->get_current()->get_key(this->current_slot);
            }

            /// Read-only reference to the current data object
            inline const data_type &data() const {
                return this->get_current()->get_value(this->current_slot);
            }

            /// return change id for current page
            inline size_t change_id() {
                return this->get_current()->get_changes();
            }

            /**
            * advances/retreats iterator by count steps
            * (optimized)
            * @param count if negative advances
            * @return this
            */
            inline self &operator-=(const long long count) {
                if (count >= 0)
                    iterator_base::_retreat(count);
                else
                    iterator_base::_advance(-count);
                return *this;
            }

            /**
             * advances/retreats iterator by count steps
             * (optimized)
             * @param count if negative retreats
             * @return this
             */
            inline self &operator+=(const long long count) {
                if (count >= 0)
                    iterator_base::_advance(count);
                else
                    iterator_base::_retreat(-count);
                return *this;
            }

            /// Prefix++ advance the iterator to the next slot
            inline self &operator++() {

                if (this->current_slot + 1 < this->get_current()->get_occupants()) {
                    ++this->current_slot;
                } else if (this->get_current()->get_next() != NULL_REF) {
                    //if (get_current().has_context()) get_current().get_context()->check_low_memory_state();

                    this->assign_current(this->get_current()->get_next());
                    set_check_point();
                    this->current_slot = 0;
                } else {
                    // this is end()
                    this->current_slot = this->get_current()->get_occupants();
                }

                return *this;
            }

            /// Postfix++ advance the iterator to the next slot
            inline self operator++(int) {

                self tmp = *this;   // copy ourselves

                if (this->current_slot + 1 < this->get_current()->get_occupants()) {
                    ++this->current_slot;
                } else if (this->get_current()->get_next() != NULL_REF) {
                    if (this->get_current().has_context()) this->get_current().get_context()->check_low_memory_state();

                    this->assign_current(this->get_current()->get_next());
                    set_check_point();
                    this->current_slot = 0;
                } else {
                    // this is end()
                    this->current_slot = this->get_current()->get_occupants();
                }

                return tmp;
            }

            /// Prefix-- backstep the iterator to the last slot
            inline self &operator--() {

                if (this->current_slot > 0) {
                    --this->current_slot;
                } else if (this->get_current()->preceding != NULL_REF) {
                    if (this->get_current().has_context()) this->get_current().get_context()->check_low_memory_state();

                    this->assign_current(this->get_current()->preceding);
                    set_check_point();
                    this->current_slot = this->get_current()->get_occupants() - 1;
                } else {
                    // this is begin()
                    this->current_slot = 0;
                }

                return *this;
            }

            /// Postfix-- backstep the iterator to the last slot
            inline self operator--(int) {

                self tmp = *this;   // copy ourselves

                if (this->current_slot > 0) {
                    --this->current_slot;
                } else if (this->get_current()->preceding != NULL_REF) {

                    //if (get_current().has_context()) get_current().get_context()->check_low_memory_state();

                    this->assign_current(this->get_current()->preceding);
                    set_check_point();
                    this->current_slot = this->get_current()->get_occupants() - 1;
                } else {
                    // this is begin()
                    this->current_slot = 0;
                }

                return tmp;
            }

            /// Equality of iterators
            inline bool operator==(const self &x) const {
                return (x.get_current() == this->get_current()) && (x.current_slot == this->current_slot);
            }

            /// Inequality of iterators
            inline bool operator!=(const self &x) const {
                return (x.get_current() != this->get_current()) || (x.current_slot != this->current_slot);
            }
        };

        /// STL-like mutable reverse iterator object for B+ tree items. The
        /// iterator points to a specific slot number in a surface.
        class reverse_iterator : public iterator_base {
        public:
            // *** Types

            /// The key type of the btree. Returned by key().
            typedef typename btree::key_type key_type;

            /// The data type of the btree. Returned by data().
            typedef typename btree::data_type data_type;

            /// The value type of the btree. Returned by operator*().
            typedef typename btree::value_type value_type;

            /// The pair type of the btree.
            typedef typename btree::pair_type pair_type;

            /// Reference to the value_type. STL required.
            typedef value_type &reference;

            /// Pointer to the value_type. STL required.
            typedef value_type *pointer;

            /// STL-magic iterator category
            typedef std::bidirectional_iterator_tag iterator_category;

            /// STL-magic
            typedef ptrdiff_t difference_type;

            /// Our own type
            typedef reverse_iterator self;

        private:
            // *** Members


            /// Friendly to the const_iterator, so it may access the two data items directly
            friend class iterator_base;

            friend class iterator;

            /// Also friendly to the const_iterator, so it may access the two data items directly
            friend class const_iterator;

            /// Also friendly to the const_iterator, so it may access the two data items directly
            friend class const_reverse_iterator;

            /// Evil! A temporary value_type to STL-correctly deliver operator* and
            /// operator->
            mutable value_type temp_value;

        public:
            // *** Methods
            ~reverse_iterator() {

            }

            /// Default-Constructor of a reverse iterator
            inline reverse_iterator()
                    : iterator_base(NULL_REF) {}

            /// Initializing-Constructor of a mutable reverse iterator
            /// TODO: should be tested
            inline reverse_iterator(const btree */*source*/, typename btree::surface_node::ptr l, unsigned short s)
                    : iterator_base(s, l) {}

            /// Copy-constructor from a mutable iterator
            inline reverse_iterator(const iterator &it)
                    : iterator_base(it) {}

            inline reverse_iterator &operator=(const reverse_iterator &other) {
                iterator_base::assign(other);
                this->assign_current(other.get_current());
                this->current_slot = other.current_slot;
                return *this;
            }

            /// Dereference the iterator, this is not a value_type& because key and
            /// value are not stored together
            inline reference operator*() const {
                BTREE_ASSERT(this->current_slot > 0);
                temp_value = pair_to_value_type()(pair_type(this->get_current()->keys()[this->current_slot - 1],
                                                            this->get_current()->values()[this->current_slot - 1]));
                return temp_value;
            }

            /// Dereference the iterator. Do not use this if possible, use key()
            /// and data() instead. The B+ tree does not stored key and data
            /// together.
            inline pointer operator->() const {
                BTREE_ASSERT(this->current_slot > 0);
                temp_value = pair_to_value_type()(pair_type(this->get_current()->keys()[this->current_slot - 1],
                                                            this->get_current()->values()[this->current_slot - 1]));
                return &temp_value;
            }

            /// Key of the current slot
            inline const key_type &key() const {
                BTREE_ASSERT(this->current_slot > 0);
                return this->get_current()->keys()[this->current_slot - 1];
            }

            /// refresh function to load the latest version of the current node
            /// usually only used for debugging
            void refesh() {
                if ((*this).get_current() != NULL_REF) {
                    this->get_current().refresh();
                }
            }

            /// Writable reference to the current data object
            /// TODO: mark node as changed for update correctness
            inline data_type &data() const {
                BTREE_ASSERT(this->current_slot > 0);
                return this->get_current()->values()[this->current_slot - 1];
            }

            /// Prefix++ advance the iterator to the next slot
            inline self &operator++() {
                if (this->current_slot > 1) {
                    --this->current_slot;
                } else if (this->get_current()->preceding != NULL_REF) {
                    this->assign_current(this->get_current()->preceding);
                    this->current_slot = this->get_current()->get_occupants();
                } else {
                    // this is begin() == rend()
                    this->current_slot = 0;
                }

                return *this;
            }

            /// Postfix++ advance the iterator to the next slot
            inline self operator++(int) {
                self tmp = *this;   // copy ourselves

                if (this->current_slot > 1) {
                    --this->current_slot;
                } else if (this->get_current()->preceding != NULL_REF) {
                    this->assign_current(this->get_current()->preceding);
                    this->current_slot = this->get_current()->get_occupants();
                } else {
                    // this is begin() == rend()
                    this->current_slot = 0;
                }

                return tmp;
            }

            /// Prefix-- backstep the iterator to the last slot
            inline self &operator--() {
                if (this->current_slot < this->get_current()->get_occupants()) {
                    ++this->current_slot;
                } else if (this->get_current()->get_next() != NULL_REF) {
                    this->assign_current(this->get_current()->get_next());
                    this->current_slot = 1;
                } else {
                    // this is end() == rbegin()
                    this->current_slot = this->get_current()->get_occupants();
                }

                return *this;
            }

            /// Postfix-- backstep the iterator to the last slot
            inline self operator--(int) {
                self tmp = *this;   // copy ourselves

                if (this->current_slot < this->get_current()->get_occupants()) {
                    ++this->current_slot;
                } else if (this->get_current()->get_next() != NULL_REF) {
                    this->assign_current(this->get_current()->get_next());
                    this->current_slot = 1;
                } else {
                    // this is end() == rbegin()
                    this->current_slot = this->get_current()->get_occupants();
                }

                return tmp;
            }

            /// Equality of iterators
            inline bool operator==(const self &x) const {
                return (x.get_current() == this->get_current()) && (x.current_slot == this->current_slot);
            }

            /// Inequality of iterators
            inline bool operator!=(const self &x) const {
                return (x.get_current() != this->get_current()) || (x.current_slot != this->current_slot);
            }
        };

        /// STL-like read-only reverse iterator object for B+ tree items. The
        /// iterator points to a specific slot number in a surface.
        class const_reverse_iterator : public iterator_base {
        public:
            // *** Types

            /// The key type of the btree. Returned by key().
            typedef typename btree::key_type key_type;

            /// The data type of the btree. Returned by data().
            typedef typename btree::data_type data_type;

            /// The value type of the btree. Returned by operator*().
            typedef typename btree::value_type value_type;

            /// The pair type of the btree.
            typedef typename btree::pair_type pair_type;

            /// Reference to the value_type. STL required.
            typedef const value_type &reference;

            /// Pointer to the value_type. STL required.
            typedef const value_type *pointer;

            /// STL-magic iterator category
            typedef std::bidirectional_iterator_tag iterator_category;

            /// STL-magic
            typedef ptrdiff_t difference_type;

            /// Our own type
            typedef const_reverse_iterator self;

        private:
            // *** Members

            /// Friendly to the const_iterator, so it may access the two data itset_childems directly.
            friend class iterator_base;

            friend class reverse_iterator;

            /// Evil! A temporary value_type to STL-correctly deliver operator* and
            /// operator->
            mutable value_type temp_value;


        public:
            // *** Methods
            ~const_reverse_iterator() {

            }

            /// Default-Constructor of a const reverse iterator
            inline const_reverse_iterator()
                    : iterator_base(NULL_REF) {}

            /// Initializing-Constructor of a const reverse iterator
            /// TODO: should be implemented properly or removed
            inline const_reverse_iterator(const btree */*source*/, const typename btree::surface_node::ptr l,
                                          unsigned short s)
                    : iterator_base(s, l) {}

            /// Copy-constructor from a mutable iterator
            inline const_reverse_iterator(const iterator &it)
                    : iterator_base(it) {}

            /// Copy-constructor from a const iterator
            inline const_reverse_iterator(const const_iterator &it)
                    : iterator_base(it) {}

            /// Copy-constructor from a mutable reverse iterator
            inline const_reverse_iterator(const reverse_iterator &it)
                    : iterator_base(it) {}

            /// Dereference the iterator. Do not use this if possible, use key()
            /// and data() instead. The B+ tree does not stored key and data
            /// together.
            inline reference operator*() const {
                BTREE_ASSERT(this->current_slot > 0);
                temp_value = pair_to_value_type()(pair_type(this->get_current()->keys()[this->current_slot - 1],
                                                            this->get_current()->values()[this->current_slot - 1]));
                return temp_value;
            }

            /// Dereference the iterator. Do not use this if possible, use key()
            /// and data() instead. The B+ tree does not stored key and data
            /// together.
            inline pointer operator->() const {
                BTREE_ASSERT(this->current_slot > 0);
                temp_value = pair_to_value_type()(pair_type(this->get_current()->keys()[this->current_slot - 1],
                                                            this->get_current()->values()[this->current_slot - 1]));
                return &temp_value;
            }

            /// Key of the current slot
            inline const key_type &key() const {
                BTREE_ASSERT(this->current_slot > 0);
                return this->get_current()->keys()[this->current_slot - 1];
            }

            /// Read-only reference to the current data object
            inline const data_type &data() const {
                BTREE_ASSERT(this->current_slot > 0);
                return this->get_current()->values()[this->current_slot - 1];
            }

            /// Prefix++ advance the iterator to the previous slot
            inline self &operator++() {
                if (this->current_slot > 1) {
                    --this->current_slot;
                } else if (this->get_current()->preceding != NULL_REF) {
                    this->assign_current(this->get_current()->preceding);
                    this->current_slot = this->get_current()->get_occupants();
                } else {
                    // this is begin() == rend()
                    this->current_slot = 0;
                }

                return *this;
            }

            /// Postfix++ advance the iterator to the previous slot
            inline self operator++(int) {
                self tmp = *this;   // copy ourselves

                if (this->current_slot > 1) {
                    --this->current_slot;
                } else if (this->get_current()->preceding != NULL_REF) {
                    this->assign_current(this->get_current()->preceding);
                    this->current_slot = this->get_current()->get_occupants();
                } else {
                    // this is begin() == rend()
                    this->current_slot = 0;
                }

                return tmp;
            }

            /// Prefix-- backstep the iterator to the next slot
            inline self &operator--() {
                if (this->current_slot < this->get_current()->get_occupants()) {
                    ++this->current_slot;
                } else if (this->get_current()->get_next() != NULL_REF) {
                    this->assign_current(this->get_current()->get_next());
                    this->current_slot = 1;
                } else {
                    // this is end() == rbegin()
                    this->current_slot = this->get_current()->get_occupants();
                }

                return *this;
            }

            /// Postfix-- backstep the iterator to the next slot
            inline self operator--(int) {
                self tmp = *this;   // copy ourselves

                if (this->current_slot < this->get_current()->get_occupants()) {
                    ++this->current_slot;
                } else if (this->get_current()->get_next() != NULL_REF) {
                    this->assign_current(this->get_current()->get_next());
                    this->current_slot = 1;
                } else {
                    // this is end() == rbegin()
                    this->current_slot = this->get_current()->get_occupants();
                }

                return tmp;
            }

            /// Equality of iterators
            inline bool operator==(const self &x) const {
                return (x.get_current() == this->get_current()) && (x.current_slot == this->current_slot);
            }

            /// Inequality of iterators
            inline bool operator!=(const self &x) const {
                return (x.get_current() != this->get_current()) || (x.current_slot != this->current_slot);
            }
        };

    public:
        // *** Small Statistics Structure

        /** A small struct containing basic statistics about the B+ tree. It can be
		* fetched using get_stats(). */
        struct tree_stats {
            /// Number of items in the B+ tree
            persist::storage::u64 tree_size = 0;

            /// Number of leaves in the B+ tree
            nst::u64 leaves = 0;

            /// Number of interior nodes in the B+ tree
            nst::u64 interiornodes = 0;

            /// Bytes used by nodes in tree
            ptrdiff_t use = 0;

            /// Bytes used by surface nodes in tree
            ptrdiff_t surface_use = 0;

            /// Bytes used by interior nodes in trestart_tree_sizee
            ptrdiff_t interior_use = 0;

            ptrdiff_t max_use = 0;

            /// iterators away
            persist::storage::u64 iterators_away = 0;

            /// number of pages changed since last flush
            size_t changes = 0;

            /// cached size of the last surface
            nst::u64 last_surface_size = 0;

            /// cached old values for optimization of transactions
            nst::u64 start_leaves = 0;
            nst::u64 start_interiornodes = 0;

            /// Base B+ tree parameter: The number of key/data slots in each surface
            static const unsigned short surfaces = btree_self::surfaceslotmax;

            /// Base B+ tree parameter: The number of key slots in each interior node.
            static const unsigned short interiorslots = btree_self::interiorslotmax;


            /// Zero initialized
            inline tree_stats(){

            }

            /// Return the total number of nodes
            inline size_type nodes() const {
                return interiornodes + leaves;
            }

            /// members to optimize small transactions
            persist::storage::u64 start_root = 0;
            persist::storage::u64 start_tree_size = 0;
            persist::storage::u64 start_head = 0;
            persist::storage::u64 start_last = 0;
            persist::storage::u64 start_last_surface_size = 0;
        };

    private:
        // *** Tree Object Data Members

        /// Pointer to the B+ tree's root node, either surface or interior node
        typename node::ptr root;

        /// Pointer to first surface in the double linked surface chain
        typename surface_node::ptr first_surface;

        /// Pointer to last surface in the double linked surface chain
        typename surface_node::ptr last_surface;

        /// Other small statistics about the B+ tree
        mutable tree_stats stats;

        /// use lz4 in mem compression otherwize use zlib

        static const bool lz4 = true;

        /// do a delete check

        static const bool delete_check = true;

        /// reload version mode. if true then pages as reloaded on the fly improving small transaction performance

        static const bool version_reload = false; // currently version reload cannot work since pages are shared

        /// Key comparison object. More comparison functions are generated from
        /// this < relation.
        key_compare key_less;

        key_interpolator key_terp;
        /// Memory allocator.
        allocator_type allocator;

        /// storage
        storage_type *storage;

        void initialize_contexts() {

            root.set_context(this);
            first_surface.set_context(this);
            last_surface.set_context(this);
            first_surface.check_zero_w = true;
            persist::storage::u64 b = 0;
            // 1 = ROOT_PAGE
            // 2 = TREE_SIZE
            // 3 = PAGE_LIST_HEAD
            // 4 = PAGE_LIST_TAIL
            // 5 = LAST_PAGE_SIZE
            if (get_storage()->get_boot_value(b, ROOT_PAGE)) {
                // TODO: this section needs many integrity checks
                restore((persist::storage::stream_address) b);
                stats.start_root = b;
                persist::storage::u64 sa = 0;
                get_storage()->get_boot_value(stats.tree_size, TREE_SIZE);
                stats.start_tree_size = stats.tree_size;
                get_storage()->get_boot_value(sa, PAGE_LIST_HEAD);
                stats.start_head = sa;
                first_surface.set_where((persist::storage::stream_address) sa);
                get_storage()->get_boot_value(sa, PAGE_LIST_TAIL);
                stats.start_last = sa;
                last_surface.set_where((persist::storage::stream_address) sa);
                // TODO: last_surface_size does not seem to be used properly
                get_storage()->get_boot_value(stats.last_surface_size, LAST_PAGE_SIZE);
                stats.start_last_surface_size = stats.last_surface_size;
                get_storage()->get_boot_value(stats.leaves, LEAF_NODES);
                get_storage()->get_boot_value(stats.interiornodes, INTERIOR_NODES);
            }

        }

    public:
        // *** Constructors and Destructor

        /// Default constructor initializing an empty B+ tree with the standard key
        /// comparison function
        explicit inline btree(storage_type &storage, const allocator_type &alloc = allocator_type())
                : root(NULL_REF), first_surface(NULL_REF), last_surface(NULL_REF), allocator(alloc), storage(&storage) {


            ++btree_totl_instances;
            initialize_contexts();
        }

        /// Constructor initializing an empty B+ tree with a special key
        /// comparison object
        explicit inline btree
                (storage_type &storage, const key_compare &kcf, const allocator_type &alloc = allocator_type()
                )
                : root(NULL_REF), first_surface(NULL_REF), last_surface(NULL_REF), key_less(kcf), allocator(alloc),
                  storage(&storage) {

            ++btree_totl_instances;
            initialize_contexts();
        }

        /// Constructor initializing a B+ tree with the range [first,last)
        template<class InputIterator>
        inline btree
                (storage_type &storage, InputIterator first, InputIterator last,
                 const allocator_type &alloc = allocator_type()
                )
                : root(NULL_REF), first_surface(NULL_REF), last_surface(NULL_REF), allocator(alloc), storage(&storage) {

            ++btree_totl_instances;
            insert(first, last);
        }

        /// Constructor initializing a B+ tree with the range [first,last) and a
        /// special key comparison object
        template<class InputIterator>
        inline btree
                (storage_type &storage, InputIterator first, InputIterator last, const key_compare &kcf,
                 const allocator_type &alloc = allocator_type()
                )
                : root(NULL_REF), first_surface(NULL_REF), last_surface(NULL_REF), key_less(kcf), allocator(alloc),
                  storage(&storage) {

            ++btree_totl_instances;
            insert(first, last);
        }

        /// Frees up all used B+ tree memory pages
        inline ~btree() {
            (*this).first_surface.check_zero_w = false;
            clear();
            --btree_totl_instances;
        }

        /// Fast swapping of two identical B+ tree objects.
        void swap(btree_self &from) {
            std::swap(root, from.root);
            std::swap(first_surface, from.first_surface);
            std::swap(last_surface, from.last_surface);
            std::swap(stats, from.stats);
            std::swap(key_less, from.key_less);
            std::swap(allocator, from.allocator);
        }

    public:
        // *** Key and Value Comparison Function Objects

        /// Function class to compare value_type objects. Required by the STL
        class value_compare {
        protected:
            /// Key comparison function from the template parameter
            key_compare key_comp;

            /// Constructor called from btree::value_comp()
            inline value_compare(key_compare kc)
                    : key_comp(kc) {}

            /// Friendly to the btree class so it may call the constructor
            friend class btree<key_type, data_type, storage_type, value_type, key_compare, key_interpolator, traits, allow_duplicates>;

        public:
            /// Function call "less"-operator resulting in true if x < y.
            inline bool operator()(const value_type &x, const value_type &y) const {
                return key_comp(x.first, y.first);
            }
        };

        /// Constant access to the key comparison object sorting the B+ tree
        inline key_compare key_comp() const {
            return key_less;
        }

        /// Constant access to a constructed value_type comparison object. Required
        /// by the STL
        inline value_compare value_comp() const {
            return value_compare(key_less);
        }

    private:
        // *** Convenient Key Comparison Functions Generated From key_less
        /// True if a < b ? constructed from key_less()
        inline bool key_smaller(const key_type &a, const key_type b) const {
            return key_less(a, b);
        }
        /// True if a <= b ? constructed from key_less()
        inline bool key_lessequal(const key_type &a, const key_type b) const {
            return !key_less(b, a);
        }

        /// True if a > b ? constructed from key_less()
        inline bool key_greater(const key_type &a, const key_type &b) const {
            return key_less(b, a);
        }

        /// True if a >= b ? constructed from key_less()
        inline bool key_greaterequal(const key_type &a, const key_type b) const {
            return !key_less(a, b);
        }

        /// True if a == b ? constructed from key_less(). This requires the <
        /// relation to be a total order, otherwise the B+ tree cannot be sorted.
        inline bool key_equal(const key_type &a, const key_type &b) const {
            /// TODO: some compilers dont optimize this very well
            /// others do
            /// return !key_less(a, b) && !key_less(b, a);
            /// Most compilers can optimize this
            return a == b;
        }

        /// saves the boot values - can npt in read only mode or when nodes are shared
        void write_boot_values() {

            if (get_storage()->is_readonly()) return;
            if (stats.changes) {

               //     ROOT_PAGE = 1,
               //     TREE_SIZE = 2,
               //     PAGE_LIST_HEAD = 3,
               //     PAGE_LIST_TAIL = 4,
               //     LAST_PAGE_SIZE = 5,
               //     LEAF_NODES = 6,
               //     INTERIOR_NODES = 7

                /// avoid letting those pigeons out
                if (stats.start_root != root.get_where()) {
                    get_storage()->set_boot_value(root.get_where(), ROOT_PAGE);
                    stats.start_root = root.get_where();
                }

                if (stats.start_tree_size != stats.tree_size) {
                    get_storage()->set_boot_value(stats.tree_size, TREE_SIZE);
                    stats.start_tree_size = stats.tree_size;
                }
                if (stats.start_head != first_surface.get_where()) {
                    get_storage()->set_boot_value(first_surface.get_where(), PAGE_LIST_HEAD);
                    stats.start_head = first_surface.get_where();
                }
                if (stats.start_last != last_surface.get_where()) {
                    get_storage()->set_boot_value(last_surface.get_where(), PAGE_LIST_TAIL);
                    stats.start_last = last_surface.get_where();

                }
                if (stats.start_last_surface_size != stats.last_surface_size) {
                    get_storage()->set_boot_value(stats.last_surface_size, LAST_PAGE_SIZE);
                    stats.start_last_surface_size = stats.last_surface_size;
                }
                if (stats.start_leaves != stats.leaves) {
                    get_storage()->set_boot_value(stats.start_leaves, LEAF_NODES);
                    stats.start_leaves = stats.last_surface_size;
                }
                if (stats.start_interiornodes != stats.interiornodes) {
                    get_storage()->set_boot_value(stats.interiornodes, INTERIOR_NODES);
                    stats.start_interiornodes = stats.interiornodes;
                }

                stats.changes = 0;
            }
        }

    public:
        // *** Allocators

        /// Return the base node allocator provided during construction.
        allocator_type get_allocator() const {
            return allocator;
        }

        /// Return the storage interface used for storage operations
        storage_type *get_storage() const {
            return storage;
        }

        /// writes all modified pages to storage only
        void flush() {

            if (stats.tree_size) {
                if (!get_storage()->is_readonly()) {

                    for (auto n = modified.begin(); n != modified.end(); ++n) {
                        this->save((*n).second, (*n).first);

                    }

                    modified.clear();
                    write_boot_values();
                }
                ///BTREE_PRINT("flushing %ld\n",flushed);
            }


        }

    private:

        void unlink_surface_nodes(size_t mloaded) {
            size_t sl = surfaces_loaded.size();
            if (sl > mloaded) {
                for (auto i = interiors_loaded.begin(); i != interiors_loaded.end(); ++i) {
                    i->second->clear_surfaces();
                }
            }
        }


        void destroy_surfaces(bool all = true) {
            
            for (auto n = surfaces_loaded.begin(); n != surfaces_loaded.end(); ++n) {
                typename surface_node::shared_ptr surface = (*n).second;
                typename surface_node::ptr saved = surface;
                if (n->first != surface->get_address()) {
                    print_err("invalid persistent address");
                    throw bad_pointer();
                }
                surface->unload_links();
                if (all || !saved.is_changed()) {
                    surface->unref();
                    surface->destroy_orphan(); // this actually destroys the surface itself
                    nodes_loaded.erase(n->first);
                } else {
                    /// surface modified
                    wrn_print("surface modified");
                    this->save((*n).second, (*n).first);
                }
            }
            surfaces_loaded.clear();
        }

        void destroy_interiors() {
            //typename interior_node::alloc_type interior_allocator;
            for (auto n = interiors_loaded.begin(); n != interiors_loaded.end(); ++n) {
                nodes_loaded.erase(n->first);
#ifdef _SAVE_ALL_INTERIORS_
                                                                                                                                        if (!get_storage()->is_readonly()) {
                    if ((*n).second->get_occupants() > 0) {
                        print_dbg("save interior anyway");
                        this->save((*n).second, (*n).first);
                    }
                }
#endif
                //interior_allocator.destroy(n->second);
                //n->second->~interior_node();
                //interior_allocator.deallocate(n->second, 1);
            }
            interiors_loaded.clear();
        }

        /// unlink nodes
        void raw_unlink_nodes() {

            this->first_surface.discard(*this);
            this->last_surface.discard(*this);
            this->root.discard(*this);

            this->first_surface.set_context(this);
            this->last_surface.set_context(this);
            this->root.set_context(this);

            /// typedef std::vector<node::ptr, ::sta::tracker<node::ptr,::sta::bt_counter>> _LinkedList;

            typename node::ptr t;
            for (auto n = nodes_loaded.begin(); n != nodes_loaded.end(); ++n) {
                if (nodes_loaded.count((*n).first) > 0) {
                    if ((*n).second == NULL_REF) {
                        print_err("cannot unlink null node");
                        throw bad_pointer();
                    } else {
                        t = (*n).second;
                        t.set_context(this);
                        t.unlink();
                    }
                }
            }

        }

        /// unlink nodes from leaf pages only
        void unlink_local_nodes() {

            raw_unlink_nodes();

        }


    public:

        /// checks if theres a low memory state and release written pages
        void check_low_memory_state() {
            if (::persist::memory_low_state) {
                reduce_use();
            }

        }

        void check_low_memory_state() const {
            if (::persist::memory_low_state) {
                print_err("cannot reduce memory");
                //reduce_use();
            }

        }

        /// writes all modified pages to storage and frees all surface nodes
        void reduce_use() {


            flush_buffers(true);

            //dbg_print("complete reducing b-tree use %lld surface pages",(nst::lld)btree_totl_surfaces);
            //
            //free_surfaces();
        }
        /// enable disable verification
        void set_verification(bool _selfverify){
            this->selfverify = _selfverify;
        }
        
        void flush_buffers(bool reduce) {
            if (reduce && stats.iterators_away > 0) {
                //return;
            }
            /// size_t nodes_before = nodes_loaded.size();
            ptrdiff_t save_tot = btree_totl_used;
            flush();
            if (reduce) {

                this->key_lookup.clear();
                size_t nodes_before = nodes_loaded.size();
                if (nodes_before > 0) {
                    size_t surfaces_before = surfaces_loaded.size();
                    if (true) {

                        if(first_surface != NULL)
                            (*this).first_surface->unload_links();
                        if(last_surface != NULL)
                            (*this).last_surface->unload_links();
                        (*this).root.unload_ptr(); /// TODO the root can possibly be a surface if the tree is still small

                        (*this).first_surface.unload_ptr();
                        (*this).last_surface.unload_ptr();

                        unlink_surface_nodes(0);
                        destroy_interiors();
                        destroy_surfaces(false);
                        this->key_lookup.clear();
                        //stats = tree_stats();
                        //initialize_contexts();
                    } else {
                        clear();
                        initialize_contexts();
                    }

                    size_t nodes_after = nodes_loaded.size();
                    size_t surfaces_after = surfaces_loaded.size();
                    if (surfaces_after < surfaces_before) {
                        print_dbg("surface nodes remaining",surfaces_after);
                    }

                    if (save_tot > btree_totl_used)
                        inf_print("total tree use %.8g MiB after flush , nodes removed %lld (%lld)remaining %lld",
                                  (double) btree_totl_used / (1024.0 * 1024.0),
                                  (long long) nodes_before - (long long) nodes_after,
                                  (long long) surfaces_before - (long long) surfaces_after,
                                  (long long) nodes_loaded.size());
                }


            }
        }

    private:
        // *** Node Object Allocation and Deallocation Functions

        void change_use(ptrdiff_t used, ptrdiff_t inode, ptrdiff_t snode) {
            stats.use += used;
            stats.interior_use += inode;
            stats.surface_use += snode;
            /// add_btree_totl_used (used);
            /// stats.report_use();
        };

        /// Return an allocator for surface_node objects
        typename surface_node::alloc_type surface_node_allocator() {
            return typename surface_node::alloc_type(allocator);
        }

        /// Return an allocator for interior_node objects
        typename interior_node::alloc_type interior_node_allocator() {
            return typename interior_node::alloc_type(allocator);
        }

        typename surface_node::shared_ptr allocate_shared_surface() {
#if 0
            typename surface_node::alloc_type surface_allocator;
            auto r = surface_allocator.allocate(1);
            //surface_allocator.construct(r);
            new (r) surface_node();
#endif

            return std::make_shared<surface_node>();
        }

        typename interior_node::shared_ptr allocate_shared_interior() {
#if 0
            typename interior_node::alloc_type interior_allocator;
            auto r = interior_allocator.allocate(1);
            //interior_allocator.construct(r);
            new (r) interior_node();
            return r;
#endif
            return std::make_shared<interior_node>();
        }

        /// Allocate and initialize a surface node
        typename surface_node::ptr
        allocate_surface(stream_address w = 0) {
            // if (nodes_loaded.count(w)) {
            //    BTREE_PRINT("btree::allocate surface loading a new version of %lld\n", (nst::lld)w);
            //}
            typename surface_node::shared_ptr pn = allocate_shared_surface();

            change_use(sizeof(surface_node), 0, sizeof(surface_node));

            pn->initialize(this);
            typename surface_node::ptr n = pn;
            if(n.create(this, w)){
                stats.leaves++; // = surfaces_loaded.size();
            }
            n->set_next_context(this);
            n->preceding.set_context(this);

            if (n.get_where()) {
                n->set_address(n.get_where());
                nodes_loaded[n.get_where()] = pn;
                pn->ref();
                surfaces_loaded[n.get_where()] = pn;
                
                print_dbg("there are",nodes_loaded.size(),"nodes loaded");
            } else {
                print_err("no address supplied");
                throw bad_pointer();
            }

            return n;
        }

        /// Allocate and initialize an interior node
        typename interior_node::ptr allocate_interior(unsigned short level, stream_address w = 0) {

            typename interior_node::shared_ptr pn = allocate_shared_interior();

            pn->initialize(this, level);
            typename interior_node::ptr n = pn;
            if(n.create(this, w)){
                stats.interiornodes++;
            }

            if (n.get_where()) {
                n->set_address(n.get_where());
                nodes_loaded[n.get_where()] = pn;
                interiors_loaded[n.get_where()] = pn;
            }

            return n;
        }

    public:
        // *** Fast Destruction of the B+ Tree
        /// reload the tree when a new transaction starts
        nst::_VersionRequests request;
        nst::_VersionRequests response;
        int reloads;

        void reload() {
            bool do_reload = false; //version_reload;
            nst::u64 sa = 0;
            nst::stream_address b = 0;
            if (do_reload) {
                ++reloads;
                if (get_storage()->get_boot_value(b, ROOT_PAGE)
                    && get_storage()->is_readonly()

                        ) {

                    if (b == root.get_where()) {
                        get_storage()->get_boot_value(stats.tree_size, TREE_SIZE);
                        get_storage()->get_boot_value(sa, PAGE_LIST_HEAD);
                        if (first_surface.get_where() != (nst::stream_address) sa) {
                            this->first_surface.unlink();
                            this->first_surface.unload();
                            this->first_surface.set_context(this);
                            this->first_surface.set_where((nst::stream_address) sa);
                        }
                        get_storage()->get_boot_value(sa, PAGE_LIST_TAIL);
                        if (last_surface.get_where() != sa) {
                            this->last_surface.unlink();
                            this->last_surface.unload();
                            this->last_surface.set_context(this);
                            this->last_surface.set_where((nst::stream_address) sa);
                        }
                        get_storage()->get_boot_value(stats.last_surface_size, LAST_PAGE_SIZE);


                        return;
                    }
                }
            } else {
                do_reload = true;
            }
            if (do_reload) {
                clear();
                initialize_contexts();

            }
        }


        /// count surface uses
        size_t count_surface_uses() const {

            return stats.iterators_away;
        }

        /// Frees all key/data pairs and all nodes of the tree
        void clear() {
            if (root != NULL_REF) {
                if (stats.iterators_away > 0) {
                    // print_err("invalid reference count");
                    //throw bad_access();
                }


                (*this).first_surface = NULL_REF;
                (*this).root = NULL_REF;
                (*this).last_surface = NULL_REF;

                //unlink_surface_nodes(0);
                destroy_interiors();
                destroy_surfaces();
                this->key_lookup.clear();
                nodes_loaded.clear();

                modified.clear();
                (*this).key_lookup.clear();
                surfaces_loaded.clear();

                stats = tree_stats();


            }

            BTREE_ASSERT(stats.tree_size == 0);
        }

        /// the root address will be valid (>0) unless this is an empty tree

        stream_address get_root_address() const {
            return root.get_where();
        }

        /// setting the root address clears any current data and sets the address of the root node

        void set_root_address(stream_address w) {
            if (w) {
                (*this).clear();

                root = (*this).load(w);
            }
        }

        void restore(stream_address r) {
            set_root_address(r);
        }


    public:
        // *** STL Iterator Construction Functions

        /// Constructs a read/data-write iterator that points to the first slot in
        /// the first surface of the B+ tree.
        inline iterator begin() {
            check_low_memory_state();
            return iterator(this, first_surface, 0);
        }

        /// Constructs a read/data-write iterator that points to the first invalid
        /// slot in the last surface of the B+ tree.
        inline iterator end() {
            int o = last_surface == NULL_REF ? 0 : last_surface->get_occupants();
            typename surface_node::ptr last = last_surface;
            last.set_context(this);
            stats.last_surface_size = last != NULL_REF ? o : 0;

            return iterator(this, last, (short) stats.last_surface_size);/// avoids loading the whole page

        }

        /// Constructs a read-only constant iterator that points to the first slot
        /// in the first surface of the B+ tree.
        inline const_iterator begin() const {
            return const_iterator(this, first_surface, 0);
        }

        /// Constructs a read-only constant iterator that points to the first
        /// invalid slot in the last surface of the B+ tree.
        inline const_iterator end() const {

            typename node::ptr last = last_surface;
            last.set_context((btree *) this);
            last = last_surface;
            return const_iterator(this, last, last.get_where() != 0 ? last->get_occupants() : 0);
        }

        /// Constructs a read/data-write reverse iterator that points to the first
        /// invalid slot in the last surface of the B+ tree. Uses STL magic.
        inline reverse_iterator rbegin() {
            check_low_memory_state();
            return reverse_iterator(this, end());
        }

        /// Constructs a read/data-write reverse iterator that points to the first
        /// slot in the first surface of the B+ tree. Uses STL magic.
        inline reverse_iterator rend() {

            check_low_memory_state();
            return reverse_iterator(this, begin());
        }

        /// Constructs a read-only reverse iterator that points to the first
        /// invalid slot in the last surface of the B+ tree. Uses STL magic.
        inline const_reverse_iterator rbegin() const {
            return const_reverse_iterator(this, end());
        }

        /// Constructs a read-only reverse iterator that points to the first slot
        /// in the first surface of the B+ tree. Uses STL magic.
        inline const_reverse_iterator rend() const {
            return const_reverse_iterator(this, begin());
        }

    private:
        // *** B+ Tree Node Binary Search Functions

        /// Searches for the first key in the node n less or equal to key. Uses
        /// binary search with an optional linear self-verification. This is a
        /// template function, because the keys array is located at different
        /// places in surface_node and interior_node.
        template<typename node_type>
        inline int find_lower(const node_type *n, const key_type &key) const {
            print_dbg("find lower tree size:",stats.tree_size,", storage name:",get_storage()->get_name());
            return n->template find_lower<key_compare>(key_less, key);
        }

        template<typename node_ptr_type>
        inline int find_lower(const node_ptr_type &n, const key_type &key) const {
            print_dbg("find lower tree size:",stats.tree_size,", storage name:",get_storage()->get_name());
            return n->template find_lower<key_compare>(key_less, key);

#ifdef _BT_CHECK
        dbg_print("btree::find_lower: on",n,"key",key," -> (" ,lo,") ",hi,", ");

			// verify result using simple linear search
			if (selfverify)
			{
				int i = n->get_occupants() - 1;
				while (i >= 0 && key_lessequal(key, n->keys()[i]))
					i--;
				i++;

				BTREE_PRINT("testfind: " << i << std::endl);
				BTREE_ASSERT(i == hi);
			}
			else
			{
				BTREE_PRINT(std::endl);
			}
#endif

        }

        /// Searches for the first key in the node n greater than key. Uses binary
        /// search with an optional linear self-verification. This is a template
        /// function, because the keys array is located at different places in
        /// surface_node and interior_node.
        template<typename node_ptr_type>
        inline int find_upper(const node_ptr_type np, const key_type &key) const {
            if (np->get_occupants() == 0) return 0;
            return _find_upper(np.rget(), key);

        }

        template<typename node_type>
        inline int _find_upper(const node_type *node, const key_type &key) const {

            int lo = 0,
                    hi = node->get_occupants() - 1;

            while (lo < hi) {
                int mid = (lo + hi) >> 1;

                if (key_less(key, node->get_key(mid))) {
                    hi = mid - 1;
                } else {
                    lo = mid + 1;
                }
            }

            if (hi < 0 || key_lessequal(node->get_key(hi), key))
                hi++;

            BTREE_PRINT("btree::find_upper: on " << n << " key " << key << " -> (" << lo << ") " << hi << ", ");

            // verify result using simple linear search
            if (selfverify) {
                int i = node->get_occupants() - 1;
                while (i >= 0 && key_less(key, node->get_key(i)))
                    i--;
                i++;

                BTREE_PRINT("btree::find_upper testfind: " << i << std::endl);
                BTREE_ASSERT(i == hi);
            } else {
                BTREE_PRINT(std::endl);
            }

            return hi;
        }

    public:
        // *** Access Functions to the Item Count

        /// Return the number of key/data pairs in the B+ tree
        inline size_type size() const {
            return stats.tree_size;
        }

        /// Returns true if there is at least one key/data pair in the B+ tree
        inline bool empty() const {
            return (size() == size_type(0));
        }

        /// Returns the largest possible size of the B+ Tree. This is just a
        /// function required by the STL standard, the B+ Tree can hold more items.
        inline size_type max_size() const {
            return size_type(-1);
        }

        /// Return a const reference to the current statistics.
        inline const struct tree_stats &get_stats() const {
            return stats;
        }

    public:
        // *** Standard Access Functions Querying the Tree by Descending to a surface

        /// Non-STL function checking whether a key is in the B+ tree. The same as
        /// (find(k) != end()) or (count() != 0).
        bool exists(const key_type &key) const {
            typename node::ptr n = root;
            if (n == NULL_REF) return false;
            int slot = 0;
            stream_address loader = 0;
            while (!n->issurfacenode()) {
                typename interior_node::ptr interior = n;
                slot = find_lower((interior_node *) interior, key);
                loader = interior.get_where();
                n = interior->childid[slot];
            }

            typename surface_node::ptr surface = n;
            slot = find_lower(surface.rget(), key);
            return (slot < surface->get_occupants() && key_equal(key, surface->keys()[slot]));
        }

        /// add a hashed key to the lookup table
        void add_hash(const typename surface_node::shared_ptr &node, int at) {
            data_type &data = node->get_value(at);
            key_type &key = node->get_key(at);
            size_t h = hash_val(key);
            if (h) {

                key_lookup[h] = cache_data(node, &key, &data);
            }
        }

        /// returns the hashvalue for caching reasons
        size_t hash_val(const key_type &key) const {
            return (persist::btree_hash<key_type>()(key));
        }

        /// fast lookup that does not always succeed
        const data_type *direct(const key_type &key) const {

            size_t h = hash_val(key);
            if (h && !key_lookup.empty()) {
                auto &r = key_lookup[h];
                if (r.key != nullptr && key_equal(key, *r.key) && is_valid(r.node)) {
                    return r.value;
                }
            }
            check_low_memory_state();

            node *n = root.get();
            if (n == NULL_REF) return NULL_REF;
            int slot = 0;
            while (!n->issurfacenode()) {

                auto *interior = static_cast<interior_node *>(n);
                slot = find_lower(interior, key);
                n = interior->get_childid(slot).get();
            }

            const surface_node *surface = static_cast<surface_node *>(n);
            slot = find_lower(surface, key);
            return (slot < surface->get_occupants() && key_equal(key, surface->get_key(slot)))
                   ? (&surface->get_value(slot)) : nullptr;
        }

        data_type *direct(const key_type &key) {
            if (!key_lookup.empty()) {
                size_t h = hash_val(key);
                if (h) {
                    auto &r = key_lookup[h];
                    if (r.key != nullptr && key_equal(key, *r.key) && is_valid(r.node)) {
                        return r.value;
                    }
                }
            }


            check_low_memory_state();
            data_type *result = nullptr;
            if (get_storage()->is_readonly()) {
                if (root == NULL_REF)
                    return NULL_REF;
                node *n = root.get();

                int slot = 0;
                while (!n->issurfacenode()) {

                    auto *interior = static_cast<interior_node *>(n);
                    slot = find_lower(interior, key);
                    n = interior->get_childid(slot).get();
                }

                auto *surface = static_cast<surface_node *>(n);
                slot = find_lower(surface, key);

                result = (slot < surface->get_occupants() && key_equal(key, surface->get_key(slot)))
                         ? (&surface->get_value(slot)) : nullptr;


            }
            return result;

        }

        /// Tries to locate a key in the B+ tree and returns an iterator to the
        /// key/data slot if found. If unsuccessful it returns end().
        iterator find(const key_type &key) {
            check_low_memory_state();

            typename node::ptr n = root;
            if (n == NULL_REF) return end();
            int slot = 0;
            while (!n->issurfacenode()) {

                typename interior_node::ptr interior = n;
                slot = find_lower(interior, key);
                n = interior->get_childid(slot);
            }

            const typename surface_node::ptr surface = n;

            slot = find_lower(surface.rget(), key);
            return (slot < surface->get_occupants() && key_equal(key, surface->get_key(slot)))
                   ? iterator(this, surface, slot) : end();
        }

        /// Tries to locate a key in the B+ tree and returns an constant iterator
        /// to the key/data slot if found. If unsuccessful it returns end().
        const_iterator find(const key_type &key) const {
            typename node::ptr n = root;
            if (n == NULL_REF) return end();
            int slot = 0;

            while (!n->issurfacenode()) {
                typename interior_node::ptr interior = n;
                slot = find_lower(interior, key);
                interior->get_childid(slot).load(interior.get_where(), slot);
                n = interior->get_childid(slot);
            }

            const typename surface_node::ptr surface = n;

            slot = find_lower(surface, key);
            return (slot < surface->get_occupants() && key_equal(key, surface->get_key(slot)))
                   ? const_iterator(this, surface, slot) : end();
        }

        /// Tries to locate a key in the B+ tree and returns the number of
        /// identical key entries found.
        size_type count(const key_type &key) const {
            typename node::ptr n = root;
            if (n == NULL_REF) return 0;
            int slot = 0;

            while (!n->issurfacenode()) {
                const typename interior_node::ptr interior = n;
                slot = find_lower(interior, key);
                n = interior->get_childid(slot);
            }
            typename surface_node::ptr surface = n;
            slot = find_lower(surface, key);
            size_type num = 0;

            while (surface != NULL_REF && slot < surface->get_occupants() && key_equal(key, surface->get_key(slot))) {
                ++num;
                if (++slot >= surface->get_occupants()) {
                    surface = surface->get_next();
                    slot = 0;
                }
            }

            return num;
        }

        /// Searches the B+ tree and returns an iterator to the first pair
        /// equal to or greater than key, or end() if all keys are smaller.
        iterator lower_bound(const key_type &key) {

            if (root == NULL_REF) return end();

            if (!root.is_loaded()) {
                print_dbg("root not loaded");
            }
            typename node::ptr n = root;


            int slot = 0;

            while (!n->issurfacenode()) {
                slot = find_lower(static_cast<interior_node *>(n.operator->()), key);
                typename interior_node::ptr interior = n;
                auto *i = static_cast<interior_node *>(n.operator->());

                n = i->get_childid(slot);

            }
            const typename surface_node::ptr surface = n;

            slot = find_lower(surface, key);

            return iterator(this, surface, slot);
        }

        /// Searches the B+ tree and returns a constant iterator to the
        /// first pair equal to or greater than key, or end() if all keys
        /// are smaller.
        const_iterator lower_bound(const key_type &key) const {
            if (root == NULL_REF) return end();
            typename interior_node::ptr n = root;
            int slot = 0;

            while (!n->issurfacenode()) {

                slot = find_lower(static_cast<interior_node *>(n.operator->()), key);
                n = static_cast<interior_node *>(n.operator->())->get_childid(slot);
            }

            const typename surface_node::ptr surface = n;

            slot = find_lower(surface, key);

            return const_iterator(this, surface, slot);
        }

        /// Searches the B+ tree and returns an iterator to the first pair
        /// greater than key, or end() if all keys are smaller or equal.
        iterator upper_bound(const key_type &key) {
            typename node::ptr n = root;
            if (n == NULL_REF) return end();
            int slot = 0;

            while (!n->issurfacenode()) {
                typename interior_node::ptr interior = n;
                slot = find_upper(interior, key);

                n = interior->get_childid(slot);
            }

            const typename surface_node::ptr surface = n;

            slot = find_upper(surface, key);

            return iterator(this, surface, slot);
        }

        /// Searches the B+ tree and returns a constant iterator to the
        /// first pair greater than key, or end() if all keys are smaller
        /// or equal.
        const_iterator upper_bound(const key_type &key) const {
            typename node::ptr n = root;
            if (n == NULL_REF) return end();
            int slot = 0;

            while (!n->issurfacenode()) {
                typename interior_node::ptr interior = n;
                slot = find_upper(interior, key);


                n = interior->childid[slot];
            }

            const typename surface_node::ptr surface = n;

            slot = find_upper(surface, key);


            return const_iterator(this, surface, slot);
        }

        /// Searches the B+ tree and returns both lower_bound() and upper_bound().
        inline std::pair<iterator, iterator> equal_range(const key_type &key) {
            return std::pair<iterator, iterator>(lower_bound(key), upper_bound(key));
        }

        /// Searches the B+ tree and returns both lower_bound() and upper_bound().
        inline std::pair<const_iterator, const_iterator> equal_range(const key_type &key) const {
            return std::pair<const_iterator, const_iterator>(lower_bound(key), upper_bound(key));
        }

    public:
        // *** B+ Tree Object Comparison Functions

        /// Equality relation of B+ trees of the same type. B+ trees of the same
        /// size and equal elements (both key and data) are considered
        /// equal. Beware of the random ordering of duplicate keys.
        inline bool operator==(const btree_self &other) const {
            return (size() == other.size()) && std::equal(begin(), end(), other.begin());
        }

        /// Inequality relation. Based on operator==.
        inline bool operator!=(const btree_self &other) const {
            return !(*this == other);
        }

        /// Total ordering relation of B+ trees of the same type. It uses
        /// std::lexicographical_compare() for the actual comparison of elements.
        inline bool operator<(const btree_self &other) const {
            return std::lexicographical_compare(begin(), end(), other.begin(), other.end());
        }

        /// Greater relation. Based on operator<.
        inline bool operator>(const btree_self &other) const {
            return other < *this;
        }

        /// Less-equal relation. Based on operator<.
        inline bool operator<=(const btree_self &other) const {
            return !(other < *this);
        }

        /// Greater-equal relation. Based on operator<.
        inline bool operator>=(const btree_self &other) const {
            return !(*this < other);
        }

    public:
        /// *** Fast Copy: Assign Operator and Copy Constructors

        /// Assignment operator. All the key/data pairs are copied
        inline btree_self &operator=(const btree_self &other) {
            if (this != &other) {
                clear();

                key_less = other.key_comp();
                allocator = other.get_allocator();

                if (other.size() != 0) {
                    stats.leaves = stats.interiornodes = 0;
                    //if (other.root)
                    //{
                    //	root = copy_recursive(other.root);
                    //}
                    stats = other.stats;
                }

                if (selfverify) verify();
            }
            return *this;
        }

        /// Copy constructor. The newly initialized B+ tree object will contain a
        /// copy of all key/data pairs.
        explicit inline btree(const btree_self &other)
        : root(NULL_REF), first_surface(NULL_REF), last_surface(NULL_REF),
          stats(other.stats),
          key_less(other.key_comp()),
          allocator(other.get_allocator()) {
            ++btree_totl_instances;
            if (size() > 0) {
                stats.leaves = stats.interiornodes = 0;
                if (other.root) {
                    print_err("[WARNING] b-tree copy constructor not implemented");
                    //throw method_not_implemented_exception();
                    //root = copy_recursive(other.root);
                }
                if (selfverify) verify();
            }
        }


    public:
        // *** Public Insertion Functions

        /// Attempt to insert a key/data pair into the B+ tree. If the tree does not
        /// allow duplicate keys, then the insert may fail if it is already
        /// present.
        inline std::pair<iterator, bool> insert(const pair_type &x) {
            return insert_start(x.first, x.second);
        }

        /// Attempt to insert a key/data pair into the B+ tree. Beware that if
        /// key_type == data_type, then the template iterator insert() is called
        /// instead. If the tree does not allow duplicate keys, then the insert may
        /// fail if it is already present.
        inline std::pair<iterator, bool> insert(const key_type &key, const data_type &data) {
            return insert_start(key, data);
        }

        /// Attempt to insert a key/data pair into the B+ tree. This function is the
        /// same as the other insert, however if key_type == data_type then the
        /// non-template function cannot be called. If the tree does not allow
        /// duplicate keys, then the insert may fail if it is already present.
        inline std::pair<iterator, bool> insert2(const key_type &key, const data_type &data) {
            return insert_start(key, data);
        }

        /// Attempt to insert a key/data pair into the B+ tree. The iterator hint
        /// is currently ignored by the B+ tree insertion routine.
        inline iterator insert(iterator /* hint */, const pair_type &x) {
            return insert_start(x.first, x.second).first;
        }

        /// Attempt to insert a key/data pair into the B+ tree. The iterator hint is
        /// currently ignored by the B+ tree insertion routine.
        inline iterator insert2(iterator /* hint */, const key_type &key, const data_type &data) {
            return insert_start(key, data).first;
        }

        /// Attempt to insert the range [first,last) of value_type pairs into the B+
        /// tree. Each key/data pair is inserted individually.
        template<typename InputIterator>
        inline void insert(InputIterator first, InputIterator last) {
            InputIterator iter = first;
            while (iter != last) {
                insert(*iter);
                ++iter;
            }
        }

    private:


        // *** Private Insertion Functions

        /// Start the insertion descent at the current root and handle root
        /// splits. Returns true if the item was inserted
        std::pair<iterator, bool> insert_start(const key_type &key, const data_type &value) {
            check_low_memory_state();

            typename node::ptr newchild;
            key_type newkey = key_type();

            if (root == NULL_REF) {
                last_surface = allocate_surface();
                first_surface = last_surface;
                root = last_surface;

            }

            std::pair<iterator, bool> r = insert_descend(root, 0, 0, key, value, &newkey, newchild);

            if (newchild != NULL_REF) {
                typename interior_node::ptr newroot = allocate_interior(root->level + 1);
                newroot->set_key(0, newkey);

                newroot->set_childid(0, root);
                newroot->set_childid(1, newchild);

                newroot->set_occupants(1);
                newroot->setup_internal_keys();
                newroot.change();
                root.change();
                root = newroot;

            }

            // increment itemcount if the item was inserted
            if (r.second) ++stats.tree_size;

#ifdef BTREE_DEBUG
            if (debug) print(std::cout);
#endif

            if (selfverify) {
                verify();
                BTREE_ASSERT(exists(key));
            }

            return r;
        }

        /**
		* @brief Insert an item into the B+ tree.
		*
		* Descend down the nodes to a surface, insert the key/data pair in a free
		* slot. If the node overflows, then it must be split and the new split
		* node inserted into the parent. Unroll / this splitting up to the root.
		*/
        std::pair<iterator, bool> insert_descend(const typename node::ptr &n, stream_address /*source*/, int /*child_at*/,
                                                 const key_type &key, const data_type &value,
                                                 key_type *splitkey, typename node::ptr &splitnode) {

            if (!n->issurfacenode()) {
                typename interior_node::ptr interior = n;

                key_type newkey = key_type();
                typename node::ptr newchild;

                int at = interior->find_lower(key_less, key);

                BTREE_PRINT("btree::insert_descend into " << interior->get_childid(at) << std::endl);


                std::pair<iterator, bool> r = insert_descend(interior->get_childid(at), interior.get_where(), at,
                                                             key, value, &newkey, newchild);

                if (newchild != NULL_REF) {
                    BTREE_PRINT(
                            "btree::insert_descend newchild with key " << newkey << " node " << newchild << " at at "
                                                                       << at << std::endl);
                    newchild.change();
                    if (interior->isfull()) {


                        split_interior_node(interior, splitkey, splitnode, at);

                        BTREE_PRINT(
                                "btree::insert_descend done split_interior: putslot: " << at << " putkey: " << newkey
                                                                                       << " upkey: " << *splitkey
                                                                                       << std::endl);

#ifdef BTREE_DEBUG
                                                                                                                                                if (debug)
                    {
                        print_node(std::cout, interior);
                        print_node(std::cout, splitnode);
                    }
#endif

                        // check if insert at is in the split sibling node
                        BTREE_PRINT("btree::insert_descend switch: " << at << " > " << interior->get_occupants() + 1
                                                                     << std::endl);


                        if (at == interior->get_occupants() + 1 &&
                            interior->get_occupants() < splitnode->get_occupants()) {
                            // special case when the insert at matches the split
                            // place between the two nodes, then the insert key
                            // becomes the split key.

                            BTREE_ASSERT(interior->get_occupants() + 1 < interiorslotmax);
                            /// mark change before any modifications

                            interior.change_before();

                            typename interior_node::ptr splitinterior = splitnode;
                            splitinterior.change_before();
                            // move the split key and it's datum into the left node
                            interior->set_key(interior->get_occupants(), *splitkey);
                            interior->set_childid(interior->get_occupants() + 1, splitinterior->get_childid(0));
                            interior->inc_occupants();
                            interior->setup_internal_keys();
                            // set new split key and move corresponding datum into right node
                            splitinterior->set_childid(0, newchild);
                            splitinterior->setup_internal_keys();
                            *splitkey = newkey;

                            return r;
                        } else if (at >= interior->get_occupants() + 1) {
                            // in case the insert at is in the newly create split
                            // node, we reuse the code below.

                            at -= interior->get_occupants() + 1;
                            interior = splitnode;
                            BTREE_PRINT("btree::insert_descend switching to splitted node " << interior << " at " << at
                                                                                            << std::endl);
                        }

                    }

                    // put pointer to child node into correct at


                    BTREE_ASSERT(at >= 0 && at <= ref->get_occupants());
                    interior.change_before();

                    int i = interior->get_occupants();
                    interior_node *ref = interior.rget();

                    while (i > at) {
                        ref->set_key(i, ref->get_key(i - 1));
                        ref->set_childid(i + 1, ref->get_childid(i));
                        i--;
                    }

                    interior->set_key(at, newkey);
                    interior->set_childid(at + 1, newchild);
                    interior->inc_occupants();
                    interior->setup_internal_keys();

                }

                return r;
            } else // n->issurfacenode() == true
            {

                typename surface_node::ptr surface = n;
                int at = surface->find_lower(key_less, key);

                if (!allow_duplicates && at < surface->get_occupants() && key_equal(key, surface->get_key(at))) {
                    surface.change();
                    return std::pair<iterator, bool>(iterator(this, surface, at), false);
                }
                if (surface->isfull()) {

                    split_surface_node(surface, splitkey, splitnode);

                    // check if insert at is in the split sibling node
                    if (at >= surface->get_occupants()) {
                        at -= surface->get_occupants();
                        surface = splitnode;

                    }

                }
                // mark node as going to change

                surface.change_before();
                // put data item into correct data at
#if DEBUG
                int i = surface->get_occupants() - 1;
                BTREE_ASSERT(i + 1 < surfaceslotmax);
#endif
                surface_node *surfactant = surface.rget();
                //std::lock_guard lg(surfactant->excl);
                surfactant->insert_(at, key, value);
                BTREE_ASSERT(at > 0);

                //i = at - 1;///
                typename surface_node::ptr splitsurface = splitnode;

                surface.next_check();

                if (splitsurface != NULL_REF && surface != splitsurface && at == surface->get_occupants() - 1) {
                    // special case: the node was split, and the insert is at the
                    // last at of the old node. then the splitkey must be
                    // updated.
                    *splitkey = key;
                }

                return std::pair<iterator, bool>(iterator(this, surface, at), true);//

            }
        }

        /// Split up a surface node into two equally-filled sibling leaves. Returns
        /// the new nodes and it's insertion key in the two parameters.
        void
        split_surface_node(typename surface_node::ptr surface, key_type *_newkey, typename node::ptr &_newsurface) {
            BTREE_ASSERT(surface->isfull());
            // mark node as going to change
            surface.change_before();

            unsigned int mid = (surface->get_occupants() >> 1);

            BTREE_PRINT("btree::split_surface_node on " << surface << std::endl);

            typename surface_node::ptr newsurface = allocate_surface();

            newsurface.change_before(); // its new

            newsurface->set_occupants(surface->get_occupants() - mid);

            newsurface->set_next(surface->get_next());
            if (newsurface->get_next() == NULL_REF) {
                BTREE_ASSERT(surface == last_surface);
                last_surface = newsurface;
            } else {
                newsurface->change_next();
                newsurface->set_next_preceding(newsurface);

            }
            for (unsigned int slot = mid; slot < surface->get_occupants(); ++slot) {
                unsigned int ni = slot - mid;
                newsurface->move_kv(ni, surface->get_key(slot), surface->get_value(slot));
            }
            surface->set_occupants(mid);
            surface->set_next(newsurface);

            newsurface->preceding = surface;
            print_dbg("split surface node",surface->get_address(),"size",surface->get_occupants(),"into",newsurface->get_address(),"size",newsurface->get_occupants());
            surface.next_check();
            newsurface.next_check();
            *_newkey = surface->get_key(surface->get_occupants() - 1);
            _newsurface = newsurface;

        }

        /// Split up an interior node into two equally-filled sibling nodes. Returns
        /// the new nodes and it's insertion key in the two parameters. Requires
        /// the slot of the item will be inserted, so the nodes will be the same
        /// size after the insert.
        void
        split_interior_node(typename interior_node::ptr interior, key_type *_newkey, typename node::ptr &_newinterior,
                            unsigned int addslot) {
            BTREE_ASSERT(interior->isfull());
            // make node as going to change
            interior.change_before();

            unsigned int mid = (interior->get_occupants() >> 1);

            BTREE_PRINT("btree::split_interior: mid " << mid << " addslot " << addslot << std::endl);

            // if the split is uneven and the overflowing item will be put into the
            // larger node, then the smaller split node may underflow
            if (addslot <= mid && mid > interior->get_occupants() - (mid + 1))
                mid--;

            BTREE_PRINT("btree::split_interior: mid " << mid << " addslot " << addslot << std::endl);

            BTREE_PRINT("btree::split_interior_node on " << interior << " into two nodes " << mid << " and "
                                                         << interior->get_occupants() - (mid + 1) << " sized"
                                                         << std::endl);

            typename interior_node::ptr newinterior = allocate_interior(interior->level);

            newinterior.change_before();
            newinterior->set_occupants(interior->get_occupants() - (mid + 1));

            for (unsigned int slot = mid + 1; slot < interior->get_occupants(); ++slot) {
                unsigned int ni = slot - (mid + 1);
                newinterior->set_key(ni, interior->get_key(slot));
                newinterior->set_childid(ni, interior->get_childid(slot));
                interior->get_childid(slot).discard(*this);
            }
            newinterior->set_childid(newinterior->get_occupants(), interior->get_childid(interior->get_occupants()));
            newinterior->setup_internal_keys();

            /// TODO: BUG: this discard causes an invalid page save
            interior->get_childid(interior->get_occupants()).discard(*this);


            interior->set_occupants(mid);
            interior->setup_internal_keys();
            interior->clear_references();

            *_newkey = interior->get_key(mid);
            _newinterior = newinterior;

        }

    private:
        // *** Support Class Encapsulating Deletion Results

        /// Result flags of recursive deletion.
        enum result_flags_t {
            /// Deletion successful and no fix-ups necessary.
            btree_ok = 0,

            /// Deletion not successful because key was not found.
            btree_not_found = 1,

            /// Deletion successful, the last key was updated so parent keyss
            /// need updates.
            btree_update_lastkey = 2,

            /// Deletion successful, children nodes were merged and the parent
            /// needs to remove the empty node.
            btree_fixmerge = 4
        };

        /// B+ tree recursive deletion has much information which is needs to be
        /// passed upward.
        struct result_t {
            /// Merged result flags
            result_flags_t flags;

            /// The key to be updated at the parent's slot
            key_type lastkey;

            /// Constructor of a result with a specific flag, this can also be used
            /// as for implicit conversion.
            inline result_t(result_flags_t f = btree_ok)
                    : flags(f), lastkey() {}

            /// Constructor with a lastkey value.
            inline result_t(result_flags_t f, const key_type &k)
                    : flags(f), lastkey(k) {}

            /// Test if this result object has a given flag set.
            inline bool has(result_flags_t f) const {
                return (flags & f) != 0;
            }

            /// Merge two results OR-ing the result flags and overwriting lastkeys.
            inline result_t &operator|=(const result_t &other) {
                flags = result_flags_t(flags | other.flags);

                // we overwrite existing lastkeys on purpose
                if (other.has(btree_update_lastkey))
                    lastkey = other.lastkey;

                return *this;
            }
        };

    public:
        // *** Public Erase Functions

        /// Erases one (the first) of the key/data pairs associated with the given
        /// key.
        bool erase_one(const key_type &key) {
            BTREE_PRINT("btree::erase_one(" << key << ") on btree size " << size() << std::endl);

            if (selfverify) verify();

            if (root == NULL_REF) return false;

            check_low_memory_state();
            
            result_t result = erase_one_descend(key, root, NULL_REF, NULL_REF, NULL_REF, NULL_REF, NULL_REF, 0);

            if (!result.has(btree_not_found)) {
                --stats.tree_size;
                print_dbg("size after erase",stats.tree_size);
            }

#ifdef BTREE_DEBUG
            if (debug) print(std::cout);
#endif
            if (selfverify) verify();

            return !result.has(btree_not_found);
        }

        /// Erases all the key/data pairs associated with the given key. This is
        /// implemented using erase_one().
        size_type erase(const key_type &key) {


            size_type c = 0;

            while (erase_one(key)) {
                ++c;
                if (!allow_duplicates) break;
            }

            return c;
        }

        /// Erase the key/data pair referenced by the iterator.
        void erase(iterator iter) {
            BTREE_PRINT("btree::erase_iter(" << iter.get_current() << "," << iter.current_slot << ") on btree size "
                                             << size() << std::endl);

            if (selfverify) verify();

            if (root == NULL_REF) return;

            check_low_memory_state();
            
            result_t result = erase_iter_descend(iter, root, NULL, NULL, NULL, NULL, NULL, 0);

            if (!result.has(btree_not_found))
                --stats.tree_size;

#ifdef BTREE_DEBUG
            if (debug) print(std::cout);
#endif
            if (selfverify) verify();
        }

#ifdef BTREE_TODO
                                                                                                                                /// Erase all key/data pairs in the range [first,last). This function is
		/// currently not implemented by the B+ Tree.
		void erase(iterator /* first */, iterator /* last */)
		{
			abort();
		}
#endif

    private:
        // *** Private Erase Functions

        /** @brief Erase one (the first) key/data pair in the B+ tree matching key.
		*
		* Descends down the tree in search of key. During the descent the parent,
		* left and right siblings and their parents are computed and passed
		* down. Once the key/data pair is found, it is removed from the surface. If
		* the surface underflows 6 different cases are handled. These cases resolve
		* the underflow by shifting key/data pairs from adjacent sibling nodes,
		* merging two sibling nodes or trimming the tree.
		*/
        result_t erase_one_descend
                (const key_type &key,
                 typename node::ptr curr,
                 typename node::ptr left, typename node::ptr right,
                 typename interior_node::ptr leftparent, typename interior_node::ptr rightparent,
                 typename interior_node::ptr parent, unsigned int parentslot
                ) {
            if (curr->issurfacenode()) {
                typename surface_node::ptr surface = curr;
                typename surface_node::ptr leftsurface = left;
                typename surface_node::ptr rightsurface = right;

                /// indicates change for copy on write semantics
                surface.change_before();

                int slot = find_lower(surface, key);

                if (slot >= surface->get_occupants() || !key_equal(key, surface->get_key(slot))) {
                    BTREE_PRINT("Could not find key " << key << " to erase." << std::endl);

                    return btree_not_found;
                }
                print_dbg("found key to erase at",slot,"on page",surface.get_where());
                BTREE_PRINT("Found key in surface " << curr << " at slot " << slot << std::endl);

                surface->erase_d(slot);

                result_t myres = btree_ok;

                // if the last key of the surface was changed, the parent is notified
                // and updates the key of this surface
                if (slot == surface->get_occupants()) {
                    if (parent != NULL_REF && parentslot < parent->get_occupants()) {
                        // indicates modification

                        parent.change_before();

                        BTREE_ASSERT(parent->get_childid(parentslot) == curr);
                        parent->set_key(parentslot, surface->get_key(surface->get_occupants() - 1));

                    } else {
                        if (surface->get_occupants() >= 1) {
                            BTREE_PRINT(
                                    "Scheduling lastkeyupdate: key " << surface->get_key(surface->get_occupants() - 1)
                                                                     << std::endl);
                            myres |= result_t(btree_update_lastkey, surface->get_key(surface->get_occupants() - 1));
                        } else {
                            BTREE_ASSERT(surface == root);
                        }
                    }
                }

                if (surface->isunderflow() && !(surface == root && surface->get_occupants() >= 1)) {
                    // determine what to do about the underflow

                    // case : if this empty surface is the root, then delete all nodes
                    // and set root to NULL.
                    if (leftsurface == NULL_REF && rightsurface == NULL_REF) {
                        BTREE_ASSERT(surface == root);
                        BTREE_ASSERT(surface->get_occupants() == 0);

                        //free_node(root.rget(), root.get_where());

                        root = surface = NULL_REF;
                        first_surface = last_surface = NULL_REF;

                        // will be decremented soon by insert_start()
                        BTREE_ASSERT(stats.tree_size == 1);
                        BTREE_ASSERT(stats.leaves == 0);
                        BTREE_ASSERT(stats.interiornodes == 0);

                        return btree_ok;
                    }
                        // case : if both left and right leaves would underflow in case of
                        // a shift, then merging is necessary. choose the more local merger
                        // with our parent
                    else if ((leftsurface == NULL_REF || leftsurface->isfew()) &&
                             (rightsurface == NULL_REF || rightsurface->isfew())) {
                        if (leftparent == parent)
                            myres |= merge_leaves(leftsurface, surface, leftparent);
                        else
                            myres |= merge_leaves(surface, rightsurface, rightparent);
                    }
                        // case : the right surface has extra data, so balance right with current
                    else if ((leftsurface != NULL_REF && leftsurface->isfew()) &&
                             (rightsurface != NULL_REF && !rightsurface->isfew())) {
                        if (rightparent == parent)
                            myres |= shift_left_surface(surface, rightsurface, rightparent, parentslot);
                        else
                            myres |= merge_leaves(leftsurface, surface, leftparent);
                    }
                        // case : the left surface has extra data, so balance left with current
                    else if ((leftsurface != NULL_REF && !leftsurface->isfew()) &&
                             (rightsurface != NULL_REF && rightsurface->isfew())) {
                        if (leftparent == parent)
                            shift_right_surface(leftsurface, surface, leftparent, parentslot - 1,this->selfverify);
                        else
                            myres |= merge_leaves(surface, rightsurface, rightparent);
                    }
                        // case : both the surface and right leaves have extra data and our
                        // parent, choose the surface with more data
                    else if (leftparent == rightparent) {
                        if (leftsurface->get_occupants() <= rightsurface->get_occupants())
                            myres |= shift_left_surface(surface, rightsurface, rightparent, parentslot);
                        else
                            shift_right_surface(leftsurface, surface, leftparent, parentslot - 1, this->selfverify);
                    } else {
                        if (leftparent == parent)
                            shift_right_surface(leftsurface, surface, leftparent, parentslot - 1, this->selfverify);
                        else
                            myres |= shift_left_surface(surface, rightsurface, rightparent, parentslot);
                    }
                }
                surface.next_check();

                return myres;
            } else // !curr->issurfacenode()
            {
                typename interior_node::ptr interior = curr;
                typename interior_node::ptr leftinterior = left;
                typename interior_node::ptr rightinterior = right;

                typename node::ptr myleft, myright;
                typename interior_node::ptr myleftparent, myrightparent;

                int slot = find_lower(interior, key);

                if (slot == 0) {
                    typename interior_node::ptr l = left;
                    myleft = (left == NULL_REF) ? NULL : l->get_childid(left->get_occupants() - 1);
                    myleftparent = leftparent;
                } else {
                    myleft = interior->get_childid(slot - 1);
                    myleftparent = interior;
                }

                if (slot == interior->get_occupants()) {
                    typename interior_node::ptr r = right;
                    myright = (right == NULL_REF) ? NULL_REF : r->get_childid(0);
                    myrightparent = rightparent;
                } else {
                    myright = interior->get_childid(slot + 1);
                    myrightparent = interior;
                }

                BTREE_PRINT("erase_one_descend into " << interior->get_childid(slot) << std::endl);

                result_t result = erase_one_descend(key,
                                                    interior->get_childid(slot),
                                                    myleft, myright,
                                                    myleftparent, myrightparent,
                                                    interior, slot);

                result_t myres = btree_ok;

                if (result.has(btree_not_found)) {
                    return result;
                }

                if (result.has(btree_update_lastkey)) {
                    if (parent != NULL_REF
                        && parentslot < parent->get_occupants()) {
                        BTREE_PRINT("Fixing lastkeyupdate: key " << result.lastkey << " into parent " << parent
                                                                 << " at parentslot " << parentslot << std::endl);

                        BTREE_ASSERT(parent->get_childid(parentslot) == curr);
                        parent.change_before();
                        parent->set_key(parentslot, result.lastkey);
                    } else {
                        BTREE_PRINT("Forwarding lastkeyupdate: key " << result.lastkey << std::endl);
                        myres |= result_t(btree_update_lastkey, result.lastkey);
                    }
                }

                if (result.has(btree_fixmerge)) {
                    // either the current node or the next is empty and should be removed
                    //const interior_node * cinterior = interior.rget();
                    if (interior->get_childid(slot)->get_occupants() != 0)
                        slot++;

                    interior.change_before();
                    // this is the child slot invalidated by the merge
                    BTREE_ASSERT(interior->get_childid(slot)->get_occupants() == 0);

                    //free_node(interior->get_childid(slot));

                    for (int i = slot; i < interior->get_occupants(); i++) {
                        interior->set_key(i - 1, interior->get_key(i));
                        interior->set_childid(i, interior->get_childid(i + 1));
                    }
                    interior->dec_occupants();

                    if (interior->level == 1) {
                        BTREE_ASSERT(slot > 0);
                        // fix split key for leaves
                        slot--;
                        typename surface_node::ptr child = interior->get_childid(slot);
                        interior->set_key(slot, child->get_key(child->get_occupants() - 1));
                    }
                }

                if (interior->isunderflow() && !(interior == root && interior->get_occupants() >= 1)) {
                    // case: the interior node is the root and has just one child. that child becomes the new root
                    if (leftinterior == NULL && rightinterior == NULL) {
                        BTREE_ASSERT(interior == root);
                        BTREE_ASSERT(interior->get_occupants() == 0);
                        interior.change_before();
                        root = interior->get_childid(0);

                        interior->set_occupants(0);
                        if(stats.interiornodes==0){
                            print_err("invalid interiors count");
                        }
                        stats.interiornodes--;
                        //free_node(interior);

                        return btree_ok;
                    }
                        // case : if both left and right leaves would underflow in case of
                        // a shift, then merging is necessary. choose the more local merger
                        // with our parent
                    else if ((leftinterior == NULL || leftinterior->isfew()) &&
                             (rightinterior == NULL || rightinterior->isfew())) {
                        if (leftparent == parent)
                            myres |= merge_interior(leftinterior, interior, leftparent, parentslot - 1, this->selfverify, stats);
                        else
                            myres |= merge_interior(interior, rightinterior, rightparent, parentslot, this->selfverify, stats);
                    }
                        // case : the right surface has extra data, so balance right with current
                    else if ((leftinterior != NULL && leftinterior->isfew()) &&
                             (rightinterior != NULL && !rightinterior->isfew())) {
                        if (rightparent == parent)
                            shift_left_interior(interior, rightinterior, rightparent, parentslot, this->selfverify);
                        else
                            myres |= merge_interior(leftinterior, interior, leftparent, parentslot - 1, this->selfverify, stats);
                    }
                        // case : the left surface has extra data, so balance left with current
                    else if ((leftinterior != NULL && !leftinterior->isfew()) &&
                             (rightinterior != NULL && rightinterior->isfew())) {
                        if (leftparent == parent)
                            shift_right_interior(leftinterior, interior, leftparent, parentslot - 1, this->selfverify);
                        else
                            myres |= merge_interior(interior, rightinterior, rightparent, parentslot, this->selfverify, stats);
                    }
                        // case : both the surface and right leaves have extra data and our
                        // parent, choose the surface with more data
                    else if (leftparent == rightparent) {
                        if (leftinterior->get_occupants() <= rightinterior->get_occupants())
                            shift_left_interior(interior, rightinterior, rightparent, parentslot, this->selfverify);
                        else
                            shift_right_interior(leftinterior, interior, leftparent, parentslot - 1, this->selfverify);
                    } else {
                        if (leftparent == parent)
                            shift_right_interior(leftinterior, interior, leftparent, parentslot - 1, this->selfverify);
                        else
                            shift_left_interior(interior, rightinterior, rightparent, parentslot, this->selfverify);
                    }
                }

                return myres;
            }
        }

        /** @brief Erase one key/data pair referenced by an iterator in the B+
		* tree.
		*
		* Descends down the tree in search of an iterator. During the descent the
		* parent, left and right siblings and their parents are computed and
		* passed down. The difficulty is that the iterator contains only a pointer
		* to a surface_node, which means that this function must do a recursive depth
		* first search for that surface node in the subtree containing all pairs of
		* the same key. This subtree can be very large, even the whole tree,
		* though in practice it would not make sense to have so many duplicate
		* keys.
		*
		* Once the referenced key/data pair is found, it is removed from the surface
		* and the same underflow cases are handled as in erase_one_descend.
		*/
        result_t erase_iter_descend(const iterator &iter,
                                    typename node::ptr curr,
                                    typename node::ptr left, typename node::ptr right,
                                    typename interior_node::ptr leftparent, typename interior_node::ptr rightparent,
                                    typename interior_node::ptr parent, unsigned int parentslot) {

            
            
            typename interior_node::ptr ileft = left;
            typename interior_node::ptr iright = right;
            
            if (curr->issurfacenode()) {
                typename surface_node::ptr surface = curr;
                typename surface_node::ptr leftsurface = left;
                typename surface_node::ptr rightsurface = right;

                // if this is not the correct surface, get next step in recursive
                // search
                if (surface != iter.get_current()) {
                    return btree_not_found;
                }

                if (iter.current_slot >= surface->get_occupants()) {
                    BTREE_PRINT("Could not find iterator (" << iter.get_current() << "," << iter.current_slot
                                                            << ") to erase. Invalid surface node?" << std::endl);

                    return btree_not_found;
                }

                int slot = iter.current_slot;

                BTREE_PRINT("Found iterator in surface " << curr << " at slot " << slot << std::endl);
                surface.change_before();
                surface->erase_d(slot);

                result_t myres = btree_ok;

                // if the last key of the surface was changed, the parent is notified
                // and updates the key of this surface
                if (slot == surface->get_occupants()) {
                    if (parent != NULL && parentslot < parent->get_occupants()) {
                        BTREE_ASSERT(parent->get_childid(parentslot) == curr);
                        parent.change_before();
                        parent->set_key(parentslot, surface->get_key(surface->get_occupants() - 1));
                    } else {
                        if (surface->get_occupants() >= 1) {
                            BTREE_PRINT(
                                    "Scheduling lastkeyupdate: key " << surface->get_key(surface->get_occupants() - 1)
                                                                     << std::endl);
                            myres |= result_t(btree_update_lastkey, surface->get_key(surface->get_occupants() - 1));
                        } else {
                            BTREE_ASSERT(surface == root);
                        }
                    }
                }

                if (surface->isunderflow() && !(surface == root && surface->get_occupants() >= 1)) {
                    // determine what to do about the underflow

                    // case : if this empty surface is the root, then delete all nodes
                    // and set root to NULL.
                    if (leftsurface == NULL && rightsurface == NULL) {
                        BTREE_ASSERT(surface == root);
                        BTREE_ASSERT(surface->get_occupants() == 0);

                        //free_node(root);

                        root = surface = NULL;
                        first_surface = last_surface = NULL;

                        // will be decremented soon by insert_start()
                        BTREE_ASSERT(stats.tree_size == 1);
                        BTREE_ASSERT(stats.leaves == 0);
                        BTREE_ASSERT(stats.interiornodes == 0);

                        return btree_ok;
                    }
                        // case : if both left and right leaves would underflow in case of
                        // a shift, then merging is necessary. choose the more local merger
                        // with our parent
                    else if ((leftsurface == NULL || leftsurface->isfew()) &&
                             (rightsurface == NULL || rightsurface->isfew())) {
                        if (leftparent == parent)
                            myres |= merge_leaves(leftsurface, surface, leftparent);
                        else
                            myres |= merge_leaves(surface, rightsurface, rightparent);
                    }
                        // case : the right surface has extra data, so balance right with current
                    else if ((leftsurface != NULL && leftsurface->isfew()) &&
                             (rightsurface != NULL && !rightsurface->isfew())) {
                        if (rightparent == parent)
                            myres |= shift_left_surface(surface, rightsurface, rightparent, parentslot);
                        else
                            myres |= merge_leaves(leftsurface, surface, leftparent);
                    }
                        // case : the left surface has extra data, so balance left with current
                    else if ((leftsurface != NULL && !leftsurface->isfew()) &&
                             (rightsurface != NULL && rightsurface->isfew())) {
                        if (leftparent == parent)
                            shift_right_surface(leftsurface, surface, leftparent, parentslot - 1, this->selfverify);
                        else
                            myres |= merge_leaves(surface, rightsurface, rightparent);
                    }
                        // case : both the surface and right leaves have extra data and our
                        // parent, choose the surface with more data
                    else if (leftparent == rightparent) {
                        if (leftsurface->get_occupants() <= rightsurface->get_occupants())
                            myres |= shift_left_surface(surface, rightsurface, rightparent, parentslot);
                        else
                            shift_right_surface(leftsurface, surface, leftparent, parentslot - 1, this->selfverify);
                    } else {
                        if (leftparent == parent)
                            shift_right_surface(leftsurface, surface, leftparent, parentslot - 1, this->selfverify);
                        else
                            myres |= shift_left_surface(surface, rightsurface, rightparent, parentslot);
                    }
                }

                return myres;
            } else // !curr->issurfacenode()
            {
                typename interior_node::ptr interior = curr;
                typename interior_node::ptr leftinterior = left;
                typename interior_node::ptr rightinterior = right;

                // find first slot below which the searched iterator might be
                // located.

                result_t result;
                int slot = find_lower(interior, iter.key());

                while (slot <= interior->get_occupants()) {
                    typename node::ptr myleft, myright;
                    typename interior_node::ptr myleftparent, myrightparent;
                    
                    
                    if (slot == 0) {
                        myleft = (ileft == NULL) ? NULL : ileft->get_childid(left->get_occupants() - 1);
                        myleftparent = leftparent;
                    } else {
                        myleft = interior->get_childid(slot - 1);
                        myleftparent = interior;
                    }

                    if (slot == interior->get_occupants()) {
                        myright = (iright == NULL) ? NULL : iright->get_childid(0);
                        myrightparent = rightparent;
                    } else {
                        myright = interior->get_childid(slot + 1);
                        myrightparent = interior;
                    }

                    BTREE_PRINT("erase_iter_descend into " << interior->get_childid(slot) << std::endl);

                    result = erase_iter_descend(iter,
                                                interior->get_childid(slot),
                                                myleft, myright,
                                                myleftparent, myrightparent,
                                                interior, slot);

                    if (!result.has(btree_not_found))
                        break;

                    // continue recursive search for surface on next slot

                    if (slot < interior->get_occupants() && key_less(interior->get_key(slot), iter.key()))
                        return btree_not_found;

                    ++slot;
                }

                if (slot > interior->get_occupants())
                    return btree_not_found;

                result_t myres = btree_ok;

                if (result.has(btree_update_lastkey)) {
                    if (parent != NULL && parentslot < parent->get_occupants()) {
                        BTREE_PRINT("Fixing lastkeyupdate: key " << result.lastkey << " into parent " << parent
                                                                 << " at parentslot " << parentslot << std::endl);

                        BTREE_ASSERT(parent->get_childid(parentslot) == curr);
                        parent.change_before();
                        parent->set_key(parentslot, result.lastkey);
                    } else {
                        BTREE_PRINT("Forwarding lastkeyupdate: key " << result.lastkey << std::endl);
                        myres |= result_t(btree_update_lastkey, result.lastkey);
                    }
                }

                if (result.has(btree_fixmerge)) {
                    // either the current node or the next is empty and should be removed
                    if (interior->get_childid(slot)->get_occupants() != 0)
                        slot++;

                    // this is the child slot invalidated by the merge
                    BTREE_ASSERT(interior->get_childid(slot)->get_occupants() == 0);

                    //interior.change_before();

                    //free_node(interior->childid[slot]);
                    interior.change_before();
                    for (int i = slot; i < interior->get_occupants(); i++) {
                        interior->set_key(i - 1, interior->get_key(i));
                        interior->set_childid(i, interior->get_childid(i + 1));
                    }
                    interior->dec_occupants();

                    if (interior->level == 1) {
                        // fix split key for children leaves
                        slot--;
                        typename surface_node::ptr child = interior->get_childid(slot);
                        interior->set_key(slot, child->get_key(child->get_occupants() - 1));
                    }
                }

                if (interior->isunderflow() && !(interior == root && interior->get_occupants() >= 1)) {
                    // case: the interior node is the root and has just one child. that child becomes the new root
                    if (leftinterior == NULL && rightinterior == NULL) {
                        BTREE_ASSERT(interior == root);
                        BTREE_ASSERT(interior->get_occupants() == 0);
                        interior.change_before();
                        root = interior->get_childid(0);

                        interior->set_occupants(0);
                        //free_node(interior);

                        return btree_ok;
                    }
                        // case : if both left and right leaves would underflow in case of
                        // a shift, then merging is necessary. choose the more local merger
                        // with our parent
                    else if ((leftinterior == NULL || leftinterior->isfew()) &&
                             (rightinterior == NULL || rightinterior->isfew())) {
                        if (leftparent == parent)
                            myres |= merge_interior(leftinterior, interior, leftparent, parentslot - 1, this->selfverify, stats);
                        else
                            myres |= merge_interior(interior, rightinterior, rightparent, parentslot, this->selfverify, stats);
                    }
                        // case : the right surface has extra data, so balance right with current
                    else if ((leftinterior != NULL && leftinterior->isfew()) &&
                             (rightinterior != NULL && !rightinterior->isfew())) {
                        if (rightparent == parent)
                            shift_left_interior(interior, rightinterior, rightparent, parentslot, this->selfverify);
                        else
                            myres |= merge_interior(leftinterior, interior, leftparent, parentslot - 1, this->selfverify, stats);
                    }
                        // case : the left surface has extra data, so balance left with current
                    else if ((leftinterior != NULL && !leftinterior->isfew()) &&
                             (rightinterior != NULL && rightinterior->isfew())) {
                        if (leftparent == parent)
                            shift_right_interior(leftinterior, interior, leftparent, parentslot - 1, this->selfverify);
                        else
                            myres |= merge_interior(interior, rightinterior, rightparent, parentslot, this->selfverify, stats);
                    }
                        // case : both the surface and right leaves have extra data and our
                        // parent, choose the surface with more data
                    else if (leftparent == rightparent) {
                        if (leftinterior->get_occupants() <= rightinterior->get_occupants())
                            shift_left_interior(interior, rightinterior, rightparent, parentslot, this->selfverify);
                        else
                            shift_right_interior(leftinterior, interior, leftparent, parentslot - 1, this->selfverify);
                    } else {
                        if (leftparent == parent)
                            shift_right_interior(leftinterior, interior, leftparent, parentslot - 1, this->selfverify);
                        else
                            shift_left_interior(interior, rightinterior, rightparent, parentslot, this->selfverify);
                    }
                }

                return myres;
            }
        }

        /// Merge two surface nodes. The function moves all key/data pairs from right
        /// to left and sets right's occupants to zero. The right slot is then
        /// removed by the calling parent node.
        result_t merge_leaves
                (typename surface_node::ptr left, typename surface_node::ptr right, typename interior_node::ptr parent
                ) {
            BTREE_PRINT("Merge surface nodes " << left << " and " << right << " with common parent " << parent << "."
                                               << std::endl);
            (void) parent;

            BTREE_ASSERT(left->issurfacenode() && right->issurfacenode());
            BTREE_ASSERT(parent->level == 1);

            BTREE_ASSERT(left->get_occupants() + right->get_occupants() < surfaceslotmax);

            /// indicates change for copy on write semantics

            left.change_before();
            right.change_before();

            for (unsigned int i = 0; i < right->get_occupants(); i++) {
                left->get_key(left->get_occupants() + i) = right->get_key(i);
                left->get_value(left->get_occupants() + i) = right->get_value(i);
            }
            left->set_occupants(left->get_occupants() + right->get_occupants());

            left->set_next(right->get_next());
            if (left->get_next() != NULL_REF) {
                left->change_next();
                left->set_next_preceding(left);
            } else {
                last_surface = left;
            }
            right->set_occupants(0);
            if(stats.leaves == 0){
                print_err("total_leaf_error");
            }
            parent->setup_internal_keys();
            stats.leaves--;
            //nodes_erased.insert(right->get_address());
            right.next_check();
            left.next_check();
            return btree_fixmerge;
        }

        /// Merge two interior nodes. The function moves all key/childid pairs from
        /// right to left and sets right's occupants to zero. The right slot is then
        /// removed by the calling parent node.
        static result_t merge_interior
                (typename interior_node::ptr left, typename interior_node::ptr right,
                 typename interior_node::ptr parent, unsigned int parentslot,
                 bool selfverify,
                 tree_stats& stats
                ) {
            BTREE_PRINT("Merge interior nodes " << left << " and " << right << " with common parent " << parent << "."
                                                << std::endl);

            BTREE_ASSERT(left->level == right->level);
            BTREE_ASSERT(parent->level == left->level + 1);

            BTREE_ASSERT(parent->get_childid(parentslot) == left);

            BTREE_ASSERT(left->get_occupants() + right->get_occupants() < interiorslotmax);

            if (selfverify) {
                // find the left node's slot in the parent's children
                unsigned int leftslot = 0;
                while (leftslot <= parent->get_occupants() && parent->get_childid(leftslot) != left)
                    ++leftslot;

                BTREE_ASSERT(leftslot < parent->get_occupants());
                BTREE_ASSERT(parent->get_childid(leftslot) == left);
                BTREE_ASSERT(parent->get_childid(leftslot + 1) == right);

                BTREE_ASSERT(parentslot == leftslot);
            }
            left.change_before();
            right.change_before();
            // retrieve the decision key from parent
            left->set_key(left->get_occupants(), parent->get_key(parentslot));
            left->inc_occupants();
            left->setup_internal_keys();
            // copy over keys and children from right
            for (unsigned int i = 0; i < right->get_occupants(); i++) {
                left->set_key(left->get_occupants() + i, right->get_key(i));
                left->set_childid(left->get_occupants() + i, right->get_childid(i));
            }
            left->set_occupants(left->get_occupants() + right->get_occupants());
            left->set_childid(left->get_occupants(), right->get_childid(right->get_occupants()));

            right->set_occupants(0);
            right->setup_internal_keys();
            left->setup_internal_keys();
            parent->setup_internal_keys();
            stats.interiornodes--;
            left.next_check();
            right.next_check();
            return btree_fixmerge;
        }

        /// Balance two surface nodes. The function moves key/data pairs from right to
        /// left so that both nodes are equally filled. The parent node is updated
        /// if possible.
        static result_t shift_left_surface
                (typename surface_node::ptr left, typename surface_node::ptr right, typename interior_node::ptr parent,
                 unsigned int parentslot
                ) {
            BTREE_ASSERT(left->issurfacenode() && right->issurfacenode());
            BTREE_ASSERT(parent->level == 1);

            BTREE_ASSERT(left->get_next() == right);
            BTREE_ASSERT(left == right->preceding);

            BTREE_ASSERT(left->get_occupants() < right->get_occupants());
            BTREE_ASSERT(parent->get_childid(parentslot, left));

            /// indicates nodes are going to change and loades latest version

            right.change_before();
            left.change_before();
            parent.change_before();

            unsigned int shiftnum = (right->get_occupants() - left->get_occupants()) >> 1;

            BTREE_PRINT("Shifting (surface) " << shiftnum << " entries to left " << left << " from right " << right
                                              << " with common parent " << parent << "." << std::endl);

            BTREE_ASSERT(left->get_occupants() + shiftnum < surfaceslotmax);

            // copy the first items from the right node to the last slot in the left node.
            for (unsigned int i = 0; i < shiftnum; i++) {
                left->get_key(left->get_occupants() + i) = right->get_key(i);
                left->get_value(left->get_occupants() + i) = right->get_value(i);
            }
            left->set_occupants(left->get_occupants() + shiftnum);

            // shift all slots in the right node to the left

            right->set_occupants(right->get_occupants() - shiftnum);
            for (int i = 0; i < right->get_occupants(); i++) {
                right->get_key(i) = right->get_key(i + shiftnum);
                right->get_value(i) = right->get_value(i + shiftnum);
            }
            left.next_check();
            right.next_check();
            // fixup parent
            if (parentslot < parent->get_occupants()) {
                parent.change_before();
                parent->set_key(parentslot, left->get_key(left->get_occupants() - 1));
                parent->setup_internal_keys();
                return btree_ok;
            } else   // the update is further up the tree
            {
                return result_t(btree_update_lastkey, left->get_key(left->get_occupants() - 1));
            }

        }

        /// Balance two interior nodes. The function moves key/data pairs from right
        /// to left so that both nodes are equally filled. The parent node is
        /// updated if possible.
        static void shift_left_interior
                (typename interior_node::ptr left, typename interior_node::ptr right,
                 typename interior_node::ptr parent, unsigned int parentslot,
                 bool selfverify
                ) {
            BTREE_ASSERT(left->level == right->level);
            BTREE_ASSERT(parent->level == left->level + 1);

            BTREE_ASSERT(left->get_occupants() < right->get_occupants());
            BTREE_ASSERT(parent->get_childid(parentslot) == left);

            unsigned int shiftnum = (right->get_occupants() - left->get_occupants()) >> 1;

            BTREE_PRINT("Shifting (interior) " << shiftnum << " entries to left " << left << " from right " << right
                                               << " with common parent " << parent << "." << std::endl);

            BTREE_ASSERT(left->get_occupants() + shiftnum < interiorslotmax);

            right.change_before();
            left.change_before();
            parent.change_before();

            if (selfverify) {
                // find the left node's slot in the parent's children and compare to parentslot

                unsigned int leftslot = 0;
                while (leftslot <= parent->get_occupants() && parent->get_childid(leftslot) != left)
                    ++leftslot;

                BTREE_ASSERT(leftslot < parent->get_occupants());
                BTREE_ASSERT(parent->get_childid(leftslot) == left);
                BTREE_ASSERT(parent->get_childid(leftslot + 1) == right);

                BTREE_ASSERT(leftslot == parentslot);
            }

            // copy the parent's decision keys() and childid to the first new key on the left
            left->set_key(left->get_occupants(), parent->get_key(parentslot));
            left->inc_occupants();

            // copy the other items from the right node to the last slots in the left node.
            for (unsigned int i = 0; i < shiftnum - 1; i++) {
                left->set_key(left->get_occupants() + i, right->get_key(i));
                left->set_childid(left->get_occupants() + i, right->get_childid(i));
            }
            left->set_occupants(left->get_occupants() + shiftnum - 1);
            // fixup parent
            parent->set_key(parentslot, right->get_key(shiftnum - 1));
            // last pointer in left
            left->set_childid(left->get_occupants(), right->get_childid(shiftnum - 1));

            // shift all slots in the right node

            right->set_occupants(right->get_occupants() - shiftnum);
            for (int i = 0; i < right->get_occupants(); i++) {
                right->set_key(i, right->get_key(i + shiftnum));
                right->set_childid(i, right->get_childid(i + shiftnum));
            }
            right->set_childid(right->get_occupants(), right->get_childid(right->get_occupants() + shiftnum));
            right->setup_internal_keys();
            left->setup_internal_keys();
            parent->setup_internal_keys();
            right.next_check();
            left.next_check();
        }

        /// Balance two surface nodes. The function moves key/data pairs from left to
        /// right so that both nodes are equally filled. The parent node is updated
        /// if possible.
        static void shift_right_surface
                (typename surface_node::ptr left, typename surface_node::ptr right, typename interior_node::ptr parent,
                 unsigned int parentslot,
                 bool selfverify
                ) {
            BTREE_ASSERT(left->issurfacenode() && right->issurfacenode());
            BTREE_ASSERT(parent->level == 1);

            BTREE_ASSERT(left->get_next() == right);
            BTREE_ASSERT(left == right->preceding);
            BTREE_ASSERT(parent->get_childid(parentslot) == left);

            BTREE_ASSERT(left->get_occupants() > right->get_occupants());

            unsigned int shiftnum = (left->get_occupants() - right->get_occupants()) >> 1;

            BTREE_PRINT("Shifting (surface) " << shiftnum << " entries to right " << right << " from left " << left
                                              << " with common parent " << parent << "." << std::endl);
            // indicates pages will be changed for surface only copy on write semantics
            right.change_before();
            left.change_before();
            parent.change_before();

            if (selfverify) {
                // find the left node's slot in the parent's children
                unsigned int leftslot = 0;
                while (leftslot <= parent->get_occupants() && parent->get_childid(leftslot) != left)
                    ++leftslot;

                BTREE_ASSERT(leftslot < parent->get_occupants());
                BTREE_ASSERT(parent->get_childid(leftslot) == left);
                BTREE_ASSERT(parent->get_childid(leftslot + 1) == right);

                BTREE_ASSERT(leftslot == parentslot);
            }


            // shift all slots in the right node

            BTREE_ASSERT(right->get_occupants() + shiftnum < surfaceslotmax);

            for (int i = right->get_occupants() - 1; i >= 0; i--) {
                right->get_key(i + shiftnum) = right->get_key(i);
                right->get_value(i + shiftnum) = right->get_value(i);
            }
            right->set_occupants(right->get_occupants() + shiftnum);

            // copy the last items from the left node to the first slot in the right node.
            for (unsigned int i = 0; i < shiftnum; i++) {
                right->get_key(i) = left->get_key(left->get_occupants() - shiftnum + i);
                right->get_value(i) = left->get_value(left->get_occupants() - shiftnum + i);
            }
            left->set_occupants(left->get_occupants() - shiftnum);

            parent->set_key(parentslot, left->get_key(left->get_occupants() - 1));
            parent->setup_internal_keys();
            left.next_check();
            right.next_check();
        }

        /// Balance two interior nodes. The function moves key/data pairs from left to
        /// right so that both nodes are equally filled. The parent node is updated
        /// if possible.
        static void shift_right_interior
                (typename interior_node::ptr left, typename interior_node::ptr right,
                 typename interior_node::ptr parent, unsigned int parentslot,
                 bool selfverify
                ) {
            BTREE_ASSERT(left->level == right->level);
            BTREE_ASSERT(parent->level == left->level + 1);

            BTREE_ASSERT(left->get_occupants() > right->get_occupants());
            BTREE_ASSERT(parent->get_childid(parentslot) == left);

            right.change_before();
            left.change_before();
            parent.change_before();

            unsigned int shiftnum = (left->get_occupants() - right->get_occupants()) >> 1;

            print_dbg("Shifting (surface)",shiftnum,"entries to right",right.get_w(),"from left",left.get_w(),"with common parent",parent.get_w());

            if (selfverify) {
                // find the left node's slot in the parent's children
                unsigned int leftslot = 0;
                while (leftslot <= parent->get_occupants() && parent->get_childid(leftslot) != left)
                    ++leftslot;

                BTREE_ASSERT(leftslot < parent->get_occupants());
                BTREE_ASSERT(parent->get_childid(leftslot) == left);
                BTREE_ASSERT(parent->get_childid(leftslot + 1) == right);

                BTREE_ASSERT(leftslot == parentslot);
            }

            // shift all slots in the right node

            BTREE_ASSERT(right->get_occupants() + shiftnum < interiorslotmax);

            right->set_childid(right->get_occupants() + shiftnum, right->get_childid(right->get_occupants()));
            for (int i = right->get_occupants() - 1; i >= 0; i--) {
                right->set_key(i + shiftnum, right->get_key(i));
                right->set_childid(i + shiftnum, right->get_childid(i));
            }
            right->set_occupants(right->get_occupants() + shiftnum);

            // copy the parent's decision keys and childid to the last new key on the right
            right->set_key(shiftnum - 1, parent->get_key(parentslot));
            right->set_childid(shiftnum - 1, left->get_childid(left->get_occupants()));

            // copy the remaining last items from the left node to the first slot in the right node.
            for (unsigned int i = 0; i < shiftnum - 1; i++) {
                right->set_key(i, left->get_key(left->get_occupants() - shiftnum + i + 1));
                right->set_childid(i, left->get_childid(left->get_occupants() - shiftnum + i + 1));
            }

            // copy the first to-be-removed key from the left node to the parent's decision slot
            parent->set_key(parentslot, left->get_key(left->get_occupants() - shiftnum));
            parent->setup_internal_keys();
            left->set_occupants(left->get_occupants() - shiftnum);

        }

#ifdef BTREE_DEBUG
                                                                                                                                public:
		// *** Debug Printing

		/// Print out the B+ tree structure with keys onto the given ostream. This
		/// function requires that the header is compiled with BTREE_DEBUG and that
		/// key_type is printable via std::ostream.
		void print(std::ostream &os) const
		{
			if (root)
			{
				print_node(os, root, 0, true);
			}
		}

		/// Print out only the leaves via the double linked list.
		void print_leaves(std::ostream &os) const
		{
			os << "leaves:" << std::endl;

			const surface_node *n = first_surface;

			while (n)
			{
				os << "  " << n << std::endl;

				n = n->get_next();
			}
		}

	private:

		/// Recursively descend down the tree and print out nodes.
		static void print_node(std::ostream &os, const node* node, unsigned int depth = 0, bool recursive = false)
		{
			for (unsigned int i = 0; i < depth; i++) os << "  ";

			os << "node " << node << " level " << node->level << " occupants " << node->get_occupants() << std::endl;

			if (node->issurfacenode())
			{
				const surface_node *surfacenode = static_cast<const surface_node*>(node);

				for (unsigned int i = 0; i < depth; i++) os << "  ";
				os << "  surface prev " << surfacenode->preceding << " next " << surfacenode->get_next() << std::endl;

				for (unsigned int i = 0; i < depth; i++) os << "  ";

				for (unsigned int slot = 0; slot < surfacenode->get_occupants(); ++slot)
				{
					os << surfacenode->keys()[slot] << "  "; // << "(data: " << surfacenode->values()[slot] << ") ";
				}
				os << std::endl;
			}
			else
			{
				const interior_node *interiornode = static_cast<const interior_node*>(node);

				for (unsigned int i = 0; i < depth; i++) os << "  ";

				for (unsigned short slot = 0; slot < interiornode->get_occupants(); ++slot)
				{
					os << "(" << interiornode->childid[slot] << ") " << interiornode->keys()[slot] << " ";
				}
				os << "(" << interiornode->childid[interiornode->get_occupants()] << ")" << std::endl;

				if (recursive)
				{
					for (unsigned short slot = 0; slot < interiornode->get_occupants() + 1; ++slot)
					{
						print_node(os, interiornode->childid[slot], depth + 1, recursive);
					}
				}
			}
		}
#endif

    public:
        // *** Verification of B+ Tree Invariants

        /// Run a thorough verification of all B+ tree invariants. The program
        /// aborts via assert() if something is wrong.
        size_t verify() const {
            key_type minkey, maxkey;
            tree_stats vstats;

            if (root != NULL_REF) {
                verify_node(root, &minkey, &maxkey, vstats);

                assert(vstats.tree_size == stats.tree_size);
                /// TODO: NB: it seems some internal pages are not unlinked after erasure
                assert(vstats.leaves == stats.leaves);
                assert(vstats.interiornodes == stats.interiornodes);

                return verify_surfacelinks();
            }
            return 0;
        }

        void set_max_use(ptrdiff_t max_use) {
            stats.max_use = std::max<ptrdiff_t>(max_use, 1024 * 1024 * 64);
        }

    private:

        /// Recursively descend down the tree and verify each node
        void verify_node(const typename node::ptr n, key_type *minkey, key_type *maxkey, tree_stats &vstats) const {
            BTREE_PRINT("verifynode " << n << std::endl);

            if (n->issurfacenode()) {
                const typename surface_node::ptr surface = n;

                assert(surface == root || !surface->isunderflow());
                assert(surface->get_occupants() > 0);
                // stats.tree_size +=

                for (unsigned short slot = 0; slot < surface->get_occupants() - 1; ++slot) {
                    assert(key_lessequal(surface->get_key(slot), surface->get_key(slot + 1)));
                }

                *minkey = surface->get_key(0);
                *maxkey = surface->get_key(surface->get_occupants() - 1);

                vstats.leaves++;
                vstats.tree_size += surface->get_occupants();
            } else // !n->issurfacenode()
            {
                const typename interior_node::ptr interior = n;
                vstats.interiornodes++;

                assert(interior == root || !interior->isunderflow());
                assert(interior->get_occupants() > 0);

                for (unsigned short slot = 0; slot < interior->get_occupants() - 1; ++slot) {
                    assert(key_lessequal(interior->get_key(slot), interior->get_key(slot + 1)));
                }

                for (unsigned short slot = 0; slot <= interior->get_occupants(); ++slot) {
                    const typename node::ptr subnode = interior->get_childid(slot);
                    key_type subminkey = key_type();
                    key_type submaxkey = key_type();

                    assert(subnode->level + 1 == interior->level);
                    verify_node(subnode, &subminkey, &submaxkey, vstats);

                    BTREE_PRINT("verify subnode " << subnode << ": " << subminkey << " - " << submaxkey << std::endl);

                    if (slot == 0)
                        *minkey = subminkey;
                    else
                        assert(key_greaterequal(subminkey, interior->get_key(slot - 1)));

                    if (slot == interior->get_occupants())
                        *maxkey = submaxkey;
                    else
                        assert(key_equal(interior->get_key(slot), submaxkey));

                    if (interior->level == 1 && slot < interior->get_occupants()) {
                        // children are leaves and must be linked together in the
                        // correct order
                        const typename surface_node::ptr surfacea = interior->get_childid(slot);
                        const typename surface_node::ptr surfaceb = interior->get_childid(slot + 1);

                        assert(surfacea->get_next() == surfaceb);
                        assert(surfacea == surfaceb->preceding);
                        (void) surfacea;
                        (void) surfaceb;
                    }
                    if (interior->level == 2 && slot < interior->get_occupants()) {
                        // verify surface links between the adjacent interior nodes
                        const typename interior_node::ptr parenta = interior->get_childid(slot);
                        const typename interior_node::ptr parentb = interior->get_childid(slot + 1);

                        const typename surface_node::ptr surfacea = parenta->get_childid(parenta->get_occupants());
                        const typename surface_node::ptr surfaceb = parentb->get_childid(0);

                        assert(surfacea->get_next() == surfaceb);
                        assert(surfacea == surfaceb->preceding);
                        (void) surfacea;
                        (void) surfaceb;
                    }
                }
            }
        }

        /// Verify the double linked list of leaves.
        size_t verify_surfacelinks() const {
            typename surface_node::ptr n = first_surface;

            assert(n->level == 0);
            assert(n == NULL_REF || n->preceding == NULL_REF);

            unsigned int testcount = 0;
            size_t pages = 0;
            while (n != NULL_REF) {
                assert(n->level == 0);
                assert(n->get_occupants() > 0);

                for (unsigned short slot = 0; slot < n->get_occupants() - 1; ++slot) {
                    assert(key_lessequal(n->get_key(slot), n->get_key(slot + 1)));
                }

                testcount += n->get_occupants();

                if (n->get_next() != NULL_REF) {
                    assert(key_lessequal(n->get_key(n->get_occupants() - 1), n->get_next()->get_key(0)));

                    assert(n == n->get_next()->preceding);
                } else {
                    assert(last_surface == n);
                }

                n = n->get_next();
                ++pages;
            }

            assert(testcount == size());
            return pages;
        }

    private:

    };
} // namespace persist

#endif // PERSIST_BTREE_H_