//
// Created by Pretorius, Christiaan on 2020-12-26.
//


#include "storage/BTGraphDB.h"
#include "storage/memory_storage_alloc.h"
#include "storage/file_storage_alloc.h"
#include "storage/transaction.h"
#include "logging/console.h"
#include "bt_tx_ctx.h"
#include <random>
#include <limits>

typedef persist::btree_map<std::string, std::string, nst::transaction> bt_t;
static std::string stage = "startup";
static bool is_error = false;
static size_t failures = 0;
static const double MICROS = 1000000.0;
static const size_t MAX_TEST = 1000000;
static const auto INVALID_MEASUREMENT_RESULT = -999999999.99;

static void test_error(const std::vector<console::_LogValue>& values){
    console::println({"[ERROR ] ", console::cat(values)});
    is_error = true;
    ++failures;
}
static void test_assert(bool expression, const std::vector<console::_LogValue>& values){
    if(!expression)
        test_error(values);
}
static void log(const std::vector<console::_LogValue>& values, std::string separator = " "){
    console::println({"[ TEST ] { ", stage, " } ", console::cat(values,separator) });
}
static auto tests_start_time = std::chrono::steady_clock::now();


static void test_stage(){
    auto end = std::chrono::steady_clock::now();
    double secs = (double) (std::chrono::duration_cast<std::chrono::microseconds>(end - tests_start_time).count()) / (MICROS);
    console::println({"[RESULT] { ", stage, " } ", (is_error ? "failed": "success")," in ",secs," seconds"});
    is_error = false;
    tests_start_time = std::chrono::steady_clock::now();
}
typedef std::vector<std::string> _PlayBack;
_PlayBack create_random_data(const size_t max_items = MAX_TEST){
    log({"creating",max_items,"random(ish) records"});
    std::string encoded;
    std::vector<std::string> playback;
    std::string chars("ABCDEFGHIJKABCDEFGHIJK"); //ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789
    std::random_device rd;
    std::mt19937 g(rd());

    for (size_t s = 0; s < max_items; ++s) {
        std::string shuffled = chars;
        std::shuffle(shuffled.begin(), shuffled.end(), g);
        playback.push_back(shuffled);
    }
    log({"data create complete"});
    return playback;
}
typedef std::vector<uint64_t> _IntPlayBack;
_IntPlayBack create_random_int_data(const size_t max_items = MAX_TEST){
    log({"creating",max_items,"random int records"});
    std::string encoded;
    _IntPlayBack playback;
    
    std::random_device rd;
    std::mt19937 g(rd());
    std::uniform_int_distribution<uint64_t> dst;
    
    for (size_t s = 0; s < max_items; ++s) {
        playback.push_back(dst(g));//
    }
    log({"int data create complete"});
    return playback;
}
typedef std::vector<uint64_t> _IntPlayBack;
_IntPlayBack create_linear_int_data(const size_t max_items = MAX_TEST){
    log({"creating",max_items,"linear int records"});
    std::string encoded;
    _IntPlayBack playback;

    for (size_t s = 0; s < max_items; ++s) {
        playback.push_back(s);//
    }
    log({"int data create complete"});
    return playback;
}
/**
 * add std::string data to t1 from playback and return the time it took 
 * @tparam _TT 
 * @param t1 
 * @param MAX_TEST 
 * @return the time it took to add data in seconds
 */

template<typename _TT,typename _PBT>
static double add_data(_TT& t1, const _PBT& playback){
    auto start = std::chrono::steady_clock::now();
    for (auto current : playback) {
        t1[current] = current;
    }
    auto end = std::chrono::steady_clock::now();
    double seconds =  (std::chrono::duration_cast<std::chrono::microseconds>(end - start).count()) / (MICROS);
    return seconds;
}

template<typename _TT,typename _PBT>
static auto verify_data(_TT& t1, const _PBT& playback){
    auto start = std::chrono::steady_clock::now();
    for (auto current : playback) {
        if(t1.count(current)==0) {
            return INVALID_MEASUREMENT_RESULT;
        }
#if 0
        auto f = t1.lower_bound(current);
        if(f == t1.end()){
            return INVALID_MEASUREMENT_RESULT;
        }
#endif
        auto j = t1.find(current);
        if(j == t1.end()){
            return INVALID_MEASUREMENT_RESULT;
        }
    }

    auto end = std::chrono::steady_clock::now();
    double seconds =  (std::chrono::duration_cast<std::chrono::microseconds>(end - start).count()) / (MICROS);
    return seconds;
}

static void test_bt(){
    stage = "b-tree";
    {
        nst::file_storage_alloc storage;
        tests::bt_tx_ctx tx(&storage); // tx constructs as started

        storage.open("test_data_1.dat");
        bt_t t1(tx);
        t1["hello"] = "how";
        t1.flush();
        t1["there"] = "are";
        t1.flush();
        tx.commit();
    }
    {
        nst::file_storage_alloc storage;
        nst::transaction tx(&storage); // tx constructs as started

        storage.open("test_data_1.dat");
        bt_t t1(tx);

        assert(t1.size()==2);
        tx.rollback();
        log({"flush size test ok"});

    }
    {
        nst::file_storage_alloc storage;
        nst::transaction tx(&storage); // tx constructs as started

        storage.open("test_data_1.dat");
        bt_t t1(tx);
        assert(t1.size()==2);
        while(t1.size() > 0){
            t1.erase(t1.begin());
        }
        if (t1.begin()!=t1.end()){
            test_error({"invalid iterator"});
            return ;
        }
        tx.rollback();

    }
    {
        nst::file_storage_alloc storage;
        nst::transaction tx(&storage); // tx constructs as started

        storage.open("test_data_1.dat");
        bt_t t1(tx);
        assert(t1.size()==2);
        while(t1.size() > 0){
            t1.erase(t1.begin().key());
        }
        if (t1.begin()!=t1.end()){
            test_error({"invalid iterator"});
            return ;
        }
        tx.rollback();

    }
}
static void test_file_alloc() {
    stage = "file alloc";
    {
        nst::file_storage_alloc storage;
        nst::transaction tx(&storage); // tx constructs as started

        storage.open("test_data.dat");
        bt_t t1(tx);
        t1["hello"] = "how";
        t1.flush();
        tx.commit();
        tx.begin();
        if (t1["hello"] != "how") {
            test_error({"pre-rollback test failed", " `", t1["hello"],  "`"} );
            return;
        } 
        t1["hello"] = "there";
        t1.flush();
        tx.rollback();

        tx.begin();
        t1.reload();
        if (t1["hello"] != "how") {
            test_error({"rollback test failed", " `", t1["hello"], "`"});
        } else {
            log({"rollback ok", "`",t1["hello"],"`"});
        }


    }
    {
        nst::file_storage_alloc storage;
        nst::transaction tx(&storage); // tx constructs as started

        storage.open("test_data.dat");
        bt_t t1(tx);
        t1["hello"] = "how";
        t1.flush();
        tx.commit();

        tx.begin();
        if (t1["hello"] != "how") {
            test_error({"commit test failed"});
            return;
        } else {

        }
        t1["hello"] = "there";
        t1.flush();
        tx.commit();
        if (t1["hello"] != "there") {
            test_error({"commit test failed"});
        } 
    }
    
    {
        nst::file_storage_alloc storage;
        nst::transaction tx(&storage); // tx constructs as started
        storage.open("./test_data.dat");
        
        bt_t t1(tx);
        t1["hello"] = "where";
        t1.flush();
        tx.commit();
        nst::transaction tx1(&storage);
        bt_t t2(tx1);
        t2["hello"] = "are1";
        t2.flush();
        tx1.commit();
    }
       
    {
        nst::file_storage_alloc storage;
        nst::transaction tx(&storage); // tx constructs as started
        storage.open("./test_data.dat");
        
        bt_t t1(tx);
        
        t1.flush();
        tx.commit();
        nst::transaction tx1(&storage);
        bt_t t2(tx1);
        
        t2.flush();
        tx1.commit();
    }
    {
        nst::file_storage_alloc storage;
        nst::transaction tx(&storage); // tx constructs as started
        storage.open("./test_data.dat");
        //nst::storage_debugging = true;
        bt_t t1(tx);
        auto start = std::chrono::steady_clock::now();
        const int MAX_ITERS = 10000;
        bool transacted = true;
        for (int i = 0; i < MAX_ITERS; ++i) {
            std::string value = "where " + std::to_string(MAX_ITERS - i);
            t1["hello"] = value;
            t1.flush();
            if (transacted) {
                if (!tx.commit()) {
                    test_error({"could not commit at tx ", i});
                    return;
                }
                if (!tx.begin()) {
                    test_error({"could not start at tx ", i});
                    return;
                }
                if (t1["hello"] != value) {
                    test_error({"failed transaction"});
                    return;
                }
            }

        }
        auto end = std::chrono::steady_clock::now();
        double secs =
                (double) (std::chrono::duration_cast<std::chrono::microseconds>(end - start).count()) / (MICROS);
        log({MAX_ITERS,"transactions, total time", secs,"secs rate", MAX_ITERS / secs," tx/sec"} );
    }
    {
        nst::file_storage_alloc storage;
        nst::transaction tx(&storage);
        storage.open("./test_data.dat");
        bt_t t1(tx);
        auto f = t1.find("hello");

        if (f != t1.end()) {
            log({"t1:", f.value()});
        }else{
            test_error({"value not found"});
            return;
        }
    }
    {
        nst::file_storage_alloc storage;
        nst::transaction tx(&storage);
        storage.open("./test_data.dat");
        bt_t t1(tx);
        auto f = t1.find("hello");

        if (f != t1.end()) {
            log({ "t1:", f.value()});
        }else{
            test_error({"value not found"});
            return;
        }
    }
    {
        nst::file_storage_alloc storage;
        nst::transaction tx1(&storage);
        nst::transaction tx2(&storage);
        storage.open("./test_data.dat");
        bt_t t1(tx1);
        bt_t t2(tx2);
        auto f1 = t1.find("hello");
        auto f2 = t2.find("hello");
        if (f1 != t1.end()) {
            log({ "t1:", f1.value()});
        }else{
            test_error({"invalid value transaction"});
            return;
        }
        if(f1 != f2){
           test_error({"invalid iterator from transaction"});
           return;
        }
        
        print_inf("starting test",1);
        t1["x"] = "t1";
        t2["x"] = "t2";
        t1.flush();
        t2.flush();
        
        tx1.commit();
        tx2.commit();
        
    }
    {
        nst::file_storage_alloc storage;
        nst::transaction tx1(&storage);
        nst::transaction tx2(&storage);
        storage.open("./test_data.dat");
        bt_t t1(tx1);
        bt_t t2(tx2);
        if(t1["x"] != t2["x"]){
            test_error({"invalid value from transaction"});
            return;
        }
        if(t1["x"] != "t1"){
            test_error({"tx isolation failure"});
            return;
        }
       
    }
    nst::storage_debugging = false;
}



static inline void test_tx_alloc() {
    stage = "tx allocation";
    nst::file_storage_alloc storage;
    nst::transaction tx(&storage); // tx constructs as started
    storage.open("./test_tx_data.dat");
    //nst::storage_debugging = true;
    auto start = std::chrono::steady_clock::now();
    const int MAX_ITERS = 1000;
    nst::u64 w = 0;
    {
        auto &buffer = tx.allocate(w, persist::storage::create);
        log({"Alloc address:", w, "alloc size", buffer.size()});
        buffer.push_back(0x00);
    }

    tx.complete();
    tx.commit();
    tx.begin();
    for (int i = 0; i < MAX_ITERS; ++i) {
        auto &buffer = tx.allocate(w, persist::storage::write);
        buffer.clear();
        buffer.resize(i);
        tx.complete();
        if (!tx.commit()) {
            test_error({"could not commit at tx ", 1});
        }
        if (!tx.begin()) {
            test_error({"could not start at tx %d", 1});
        }

    }
    auto &buffer = tx.allocate(w, persist::storage::read);
    log({"persisted buffer size", buffer.size()});
    tx.complete();
    auto end = std::chrono::steady_clock::now();
    double secs = (double) (std::chrono::duration_cast<std::chrono::microseconds>(end - start).count()) / (MICROS);
    log({ MAX_ITERS, "transactions, current", secs,"secs rate", MAX_ITERS / secs, "tx/sec"} );


}



static inline void test_memory_rate() {
    stage = "persist::btree_map<std::string, std::string> memory flush rate";
    log({""});
    memory_storage_alloc storage;
    typedef persist::btree_map<std::string, std::string, memory_storage_alloc> bt_t;

    //nst::storage_debugging = true;
    bt_t t1(storage);

    for (int i = 0; i < 200; ++i) {
        t1[std::to_string(i)] = "where " + std::to_string(i);
        t1.flush();
    }
    auto start = std::chrono::steady_clock::now();
    const int MAX_ITERS = 100000;
    for (int i = 0; i < MAX_ITERS; ++i) {
        t1["hello"] = "where " + std::to_string(MAX_ITERS - i);
        t1.flush();
    }
    auto end = std::chrono::steady_clock::now();
    double secs = (double) (std::chrono::duration_cast<std::chrono::microseconds>(end - start).count()) / (MICROS);
    log({MAX_ITERS,"transactions, current", secs,"secs rate", MAX_ITERS / secs,"tx/sec" });
    log({"Size:", t1.size()});

}

static inline void test_std() {
    stage = "std::map<uint64_t, uint64_t> speed";
    typedef std::map<uint64_t, uint64_t> st_t;
    st_t t1;

    auto data = create_random_int_data(MAX_TEST);

    double seconds = add_data(t1, data);
    log({MAX_TEST, "added to std::map, bench total", seconds, "secs", MAX_TEST / seconds,"items/sec"});
    seconds = verify_data(t1,data);
    log({MAX_TEST, "read to std::map, bench total", seconds, "secs", MAX_TEST / seconds,"items/sec"});
}

static inline void test_int_memory() {
    stage = "persist::btree_map<uint64_t, uint64_t, memory_storage_alloc> random speed";
    memory_storage_alloc storage;
    typedef persist::btree_map<uint64_t, uint64_t, memory_storage_alloc> bt_t;
    bt_t t1(storage);

    auto data = create_random_int_data(MAX_TEST);

    double seconds = add_data(t1, data);
    log({MAX_TEST, "added to persist::btree_map, bench total", seconds, "secs", MAX_TEST / seconds,"items/sec"});
    seconds = verify_data(t1,data);
    log({MAX_TEST, "read to persist::btree_map, bench total", seconds, "secs", MAX_TEST / seconds,"items/sec"});
}
static inline void test_l_int_memory() {
    stage = "persist::btree_map<uint64_t, uint64_t, memory_storage_alloc> linear speed";
    memory_storage_alloc storage;
    typedef persist::btree_map<uint64_t, uint64_t, memory_storage_alloc> bt_t;
    bt_t t1(storage);

    auto data = create_linear_int_data(MAX_TEST);

    double seconds = add_data(t1, data);
    log({MAX_TEST, "added to persist::btree_map, bench total", seconds, "secs", MAX_TEST / seconds,"items/sec"});
    seconds = verify_data(t1,data);
    log({MAX_TEST, "added to persist::btree_map, bench total", seconds, "secs", MAX_TEST / seconds,"items/sec"});
}

static inline void test_block_record_int_memory() {
    stage = "persist::btree_map<uint64_t, uint64_t, memory_storage_alloc> linear speed";
    memory_storage_alloc storage;
    typedef persist::btree_map<uint64_t, std::string, memory_storage_alloc> bt_t;
    bt_t t1(storage);

    auto data = create_linear_int_data(MAX_TEST);

    double seconds = add_data(t1, data);
    log({MAX_TEST, "added to persist::btree_map, bench total", seconds, "secs", MAX_TEST / seconds,"items/sec"});
    seconds = verify_data(t1,data);
    log({MAX_TEST, "read to persist::btree_map, bench total", seconds, "secs", MAX_TEST / seconds,"items/sec"});
}

static inline void test_std_string() {
    stage = "std::map <std::string,std::string> speed";
    typedef std::map<std::string, std::string> st_t;
    st_t t1;

    auto data = create_random_data(MAX_TEST);

    double seconds = add_data(t1, data);
    log({MAX_TEST, "added to std::map, bench total", seconds, "secs", MAX_TEST / seconds,"items/sec, items in map", t1.size()});
    seconds = verify_data(t1,data);
    log({MAX_TEST, "added to std::map, bench total", seconds, "secs", MAX_TEST / seconds,"items/sec, items in map", t1.size()});
}

static void test_memory_speed() {
    stage = "persist::btree_map<uint64_t, uint64_t, memory_storage_alloc>";

    memory_storage_alloc storage;
    typedef persist::btree_map<uint64_t, uint64_t, memory_storage_alloc> bt_t;
    bt_t t1(storage);
    std::string encoded;
    auto data = create_random_int_data(MAX_TEST);

    double seconds = add_data(t1, data);

    
    log({MAX_TEST, "added to persist::btree_map, bench total", seconds, "secs rate", MAX_TEST / seconds, "items/sec"});
    t1.flush();
    log({MAX_TEST, "added to persist::btree_map, bench total with flush",storage.data_values.size(),"pages to compressed memory", seconds, "secs rate", MAX_TEST / seconds, "items/sec"});

    test_assert(t1.size() == MAX_TEST, {"invalid size"});
    size_t remaining = MAX_TEST;
    for(auto k : data){
        t1.erase(k);
        if(remaining % (MAX_TEST/10) == 0)
            t1.reduce_use();
        if(--remaining == MAX_TEST/2) break;
    }
    
    test_assert(t1.size() == MAX_TEST/2, {"invalid size"});
    while(t1.size() > 0){
        t1.erase(--t1.end());
        ++remaining;
        if(remaining % (MAX_TEST/10) == 0)
            t1.reduce_use();
    }
    test_assert(t1.size()==0, {"invalid size"});
    
}
static void test_memory_verify() {
    stage = "persist::btree_map<uint64_t, uint64_t,...> verify";

    memory_storage_alloc storage;
    typedef persist::btree_map<uint64_t, uint64_t, memory_storage_alloc> bt_t;
    bt_t t1(storage);
    //t1.enable_verification();
    std::string encoded;
    auto data = create_random_int_data(MAX_TEST);

    double seconds = add_data(t1, data);


    log({MAX_TEST, "added to persist::btree_map, bench total", seconds, "secs rate", MAX_TEST / seconds, "items/sec"});
    t1.flush();
    log({MAX_TEST, "added to persist::btree_map, bench total with flush",storage.data_values.size(),"pages to compressed memory", seconds, "secs rate", MAX_TEST / seconds, "items/sec"});

    test_assert(t1.size() == MAX_TEST, {"invalid size"});
    size_t remaining = MAX_TEST;
    for(auto k : data){
        t1.erase(k);
        test_assert(t1.count(k) == 0, {"key or count invalid"});
        if(remaining % (MAX_TEST/10) == 0)
            t1.reduce_use();
        if(--remaining == MAX_TEST/2) break;
    }
    
    log({MAX_TEST, "starting verification",storage.data_values.size()});
    t1.enable_verification();
    auto pages_verified = t1.verify();
    test_assert( pages_verified != 0, {"failed verification"});;
    t1.disable_verification();
    log({MAX_TEST, "completed verification",pages_verified});
    //for(bt_t::reverse_iterator r = t1.rbegin(); r != t1.rend();++r){
        
    //}
    size_t is = 0;
    for(bt_t::const_iterator r = t1.begin(); r != t1.end();++r){
        ++is;
    }
    test_assert(is == t1.size(),{"iterator failure"});
    is = 0;
    for(bt_t::const_iterator r = t1.end(); r != t1.begin();--r){
        ++is;
    }
    test_assert(is == t1.size(),{"iterator failure"});
    is = 0;
    for(bt_t::iterator r = t1.begin(); r != t1.end();++r){
        ++is;
    }
    test_assert(is == t1.size(),{"iterator failure"});
    is = 0;
    for(bt_t::iterator r = t1.end(); r != t1.begin();--r){
        ++is;
    }
    test_assert(is == t1.size(),{"iterator failure"});
    test_assert(t1.size() == MAX_TEST/2, {"invalid size"});
    while(t1.size() > 0){
        auto l = t1.lower_bound((--t1.end()).key());
        auto u = t1.upper_bound(l.key());
        test_assert(l != t1.end(),{"lower bound should not the end"});
        test_assert(u == t1.end(),{"upper bound should be the end"});
        auto k = l.key();
        size_t e = t1.erase(k);
        test_assert(e == 1, {"key not deleted"});
        test_assert(t1.count(k) == 0, {"key or count invalid"});
        l = t1.lower_bound(k); // first not less than k
        u = t1.upper_bound(k); // first one larger than k

        if(t1.size() != 0)
            test_assert(l == t1.end() && u == t1.end(), {"lower bound should be end"});
        else
            test_assert(l == t1.begin() && l == t1.end(),{"an empty tree beginning an end should be the same"});
        ++remaining;
        if(remaining % (MAX_TEST/10) == 0)
            t1.reduce_use();
    }
    test_assert(t1.size() == 0, {"invalid size"});

}
static void test_memory_string_speed() {
    stage = "b-tree in-memory <std::string,std::string> speed";

    memory_storage_alloc storage;
    typedef persist::btree_map<std::string, std::string, memory_storage_alloc> bt_t;
    bt_t t1(storage);
    std::string encoded;
    auto data = create_random_data(MAX_TEST);

    double seconds = add_data(t1, data);
    log({MAX_TEST, "added, bench total", seconds, "secs rate", MAX_TEST / seconds, "items/sec, items in map",t1.size()});
    seconds = verify_data(t1,data);

    log({MAX_TEST, "read, bench total", seconds, "secs rate", MAX_TEST / seconds, "items/sec, items in map",t1.size()});
    //t1.flush();
    //log({MAX_TEST, "added, bench total with flush",storage.data_values.size(),"pages to compressed memory", seconds, "secs rate", MAX_TEST / seconds, "items/sec"});

}
static void test_memory() {
    stage = "memory";

    memory_storage_alloc storage;
    typedef persist::btree_map<std::string, std::string, memory_storage_alloc> bt_t;
    bt_t t1(storage);
    std::string encoded;
    auto data = create_random_data(MAX_TEST);
   
    double seconds = add_data(t1, data);
    log({MAX_TEST,"added, bench total", seconds,"secs rate", MAX_TEST / seconds,"items/sec" });
    seconds = verify_data(t1,data);
    //t1.flush();
    log({MAX_TEST,"read, bench total", seconds,"secs rate", MAX_TEST / seconds,"items/sec" });
    t1.flush();
    bt_t::iterator test;
    size_t errors = 0;
   
    for (auto value : data) {
        test = t1.find(value);
        if (test == t1.end() || test.key() != value|| test.value() != value) {
            ++errors;
            test_error({"data not found",value});
            return;
        }
    }
    test = t1.end();
    nst::u64 iters = 0;
    for(bt_t::const_iterator c = t1.begin(); c != t1.end(); ++c){
        if(test != t1.end()){
            if(c.key() < test.key()){
                test_error({"key order error",c.key(),test.key()});
                return;
            }
        }
        if(c == t1.end()){
            test_error({"early end"});
            return;
        }
        test = t1.find(c.key());
        if (test == t1.end() || test.key() != c.key()) {
            test_error({"iterator data not found",c.key()});
            return;
        }
        ++iters;
    }
    if(iters != t1.size()){
        test_error({"size - iteration difference",iters,t1.size()});
        return;
    }
    log({"forward iteration test complete", iters,"iterations"});
    t1.flush();
   
    t1["hello"] = "there";

    t1["hi"] = "you";
    t1.flush();
    auto f = t1.find("hello");

    if (f != t1.end()) {
        log({"t1:", f.value()});
    }

    size_t bytes = 0;
    for (auto page: storage.data_values) {
        bytes += page.second->size();
    }
    double mb = ((double) bytes / (1024.0f * 1024.0f));
    log({bytes / storage.data_values.size() ,"avg. Bytes in", mb ,"mb total"
              , storage.data_values.size(),  "compressed pages"});

    bt_t t2(storage);
    auto f2 = t2.find("hello");

    if (f2 != t2.end()) {
        log({"t2:" ,f2.value()});
    }
    log({"btree test complete"});
}

static int test() {
    bool full_test = false;
    log({"starting tests"});
    if(!full_test){
        //test_memory_verify();
        //test_stage();
        //test_memory_string_speed();
        //test_stage();

        //test_l_int_memory();
        //test_stage();
        test_std();
        test_stage();
        test_int_memory();
        test_stage();
        stage = "speed";
    }else{
        test_memory_rate();
        test_stage();
        test_memory();
        test_stage();
        test_std();
        test_stage();
        test_int_memory();
        test_stage();
        test_l_int_memory();
        test_stage();
        test_memory_verify();
        test_stage();
        test_memory_speed();
        test_stage();
        test_std_string();
        test_stage();
        test_memory_string_speed();
        test_stage();
        test_bt();
        test_stage();
        test_file_alloc();
        test_stage();
        test_tx_alloc();
        test_stage();
        stage = "all";
    }

    if(failures > 0)
        log({"tests failed",failures,"times"});
    else
        log({"test complete"});
    return failures > 0 ? 1 : 0;
}

int main(int /*argc*/, char */*argv*/[]) {
    return test();
}