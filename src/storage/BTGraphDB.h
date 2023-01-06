//
// Created by Pretorius, Christiaan on 2020-06-09.
//

#ifndef REPLIFS_BT_GRAPHDB_H
#define REPLIFS_BT_GRAPHDB_H

#include <mutex>
#include <string>
#include <iostream>
#include "transaction.h"
#include "memory_storage_alloc.h"
#include "persist/storage/basic_storage.h"

namespace replifs {

    /**
     * encodes at 8 bits/byte into orderable buffer
     * @tparam _PrimitiveInteger
     * @param todo
     * @param value
     * @return
     */
    template<typename _PrimitiveInteger>
    size_t encode(std::string &todo, _PrimitiveInteger value) {
        constexpr uint32_t vsize = sizeof(_PrimitiveInteger);
        _PrimitiveInteger bits = vsize << 3;
        for (uint32_t i = 0; i < vsize; ++i) {
            bits -= 8;
            todo.push_back((char) (uint8_t) (value >> bits));
        }
        return todo.size();
    }

    template<typename _PrimitiveInteger>
    size_t decode(_PrimitiveInteger &r, const std::string &temp, size_t offset) {
        return decode(r, temp.data(), temp.size(), offset);
    }

    template<typename _PrimitiveInteger>
    size_t decode(_PrimitiveInteger &r, const char *temp, size_t size, size_t offset) {
        constexpr uint32_t vsize = sizeof(_PrimitiveInteger);
        if (offset + vsize > size) {
            std::cerr << "Decoding data from invalid string" << std::endl;
            return 0;
        }
        r = 0;
        constexpr _PrimitiveInteger mask = 0xffL;
        _PrimitiveInteger bits = vsize << 3;
        for (uint32_t i = 0; i < vsize; ++i) {
            bits -= 8;
            r += ((temp[offset + i] & mask) << bits);
        }
        return vsize;
    }

    struct KeyTool {
        std::string temp;

        template<typename _PrimitiveInteger>
        void ipend(_PrimitiveInteger value) {
            encode(temp, value);
        }

        void append(const std::string &value) {
            temp.append(value);
        }

        const char *data() const {
            return temp.c_str();
        }

        void clear() {
            temp.clear();
        }

    };

    struct BtDb {
        //mutable memory_storage_alloc storage;

        typedef persist::btree_map<std::string, nst::u64, nst::transaction> bt_t;
        mutable nst::file_storage_alloc storage{"./bt_repli_data.dat"};
        mutable nst::transaction tx{&storage}; // tx constructs as started
        mutable bt_t data{tx};
        mutable bt_t::iterator data_ptr;
        mutable bt_t::iterator update_ptr;

        BtDb() {

        }

        BtDb(const char *) {

        }

        bool is_open() const {
            return true;
        }

        bool open(const std::string &/*name*/) const {
            return storage.is_open(); //storage.open(name);
        }

        bool open(const char *name) const {
            return storage.open(name);
        }

        bool put(const std::string &k, const char *buf, size_t size, size_t intro_offset) {
            update_ptr = data.insert(k, 0).first;
            nst::u64 v_address = update_ptr.value();
            auto action = v_address ? nst::storage_action::write : nst::storage_action::create;

            nst::buffer_type *buff = &tx.allocate(v_address, action);

            if (intro_offset + size < buff->size()) {
                memcpy(&(*buff)[intro_offset], buf, size);
            } else if (intro_offset > buff->size()) {
                tx.complete();
                print_err("invalid intro offset");
                return false;
            } else {
                if (buff->empty()) {
                    buff->reserve(1024ull * 32ull);
                }
                buff->resize(intro_offset + size);
                memcpy(&(*buff)[intro_offset], buf, size);
            }


            update_ptr.data() = v_address;
            tx.complete();
            return true;
        }

        bool put(const std::string &k, const std::string &v) {
            update_ptr = data.insert(k, 0).first;
            nst::u64 v_address = update_ptr.value();
            auto action = v_address ? nst::storage_action::write : nst::storage_action::create;
            auto &buff = tx.allocate(v_address, action);
            buff.clear();
            std::copy(v.begin(), v.end(), std::back_inserter(buff));
            update_ptr.data() = v_address;
            tx.complete();
            return true;
        }

        bool get(const std::string &k, std::string &v) const {
            data_ptr = data.find(k);
            if (data_ptr == data.end()) return false;
            nst::u64 va = data_ptr.data();
            if (va == nst::u64()) v.clear();
            auto &buff = tx.allocate(va, nst::storage_action::write);
            v.clear();
            std::copy(buff.begin(), buff.end(), std::back_inserter(v));
            tx.complete();
            return true;
        }

        bool get(char *into, size_t size, size_t offset, const std::string &k) const {
            data_ptr = data.find(k);
            if (data_ptr == data.end()) return false;
            nst::u64 va = data_ptr.data();
            if (va == nst::u64()) {
                return false;
            };
            auto &buff = tx.allocate(va, nst::storage_action::write);
            if (buff.size() < size + offset) {
                print_err("offset or size error");
                tx.complete();
                return false;
            }
            memcpy(into, &buff[offset], size);
            tx.complete();
            return true;
        }

        bool remove(const std::string &k) const {
            data_ptr = data.find(k);
            if (data_ptr == data.end()) return false;
            nst::u64 va = data_ptr.data();
            if (va) {
                auto &buff = tx.allocate(va, nst::storage_action::write);
                buff.clear();
                tx.complete();
            }
            data.erase(k);
            return true;
        }

        struct iterator {
            bt_t *bt;
            nst::transaction *tx;
            bt_t::iterator iter;
            std::string eval;

            iterator(bt_t *bt, nst::transaction *tx)
                    : bt(bt), tx(tx) {
                iter = bt->begin();
            }

            iterator(bt_t *bt, nst::transaction *tx, const std::string &lb)
                    : bt(bt), tx(tx) {
                iter = bt->lower_bound(lb);
            }

            std::string &value() {
                eval.clear();
                nst::u64 va = iter.data();
                if (!va) return eval;
                auto &buff = tx->allocate(va, nst::storage_action::write);
                std::copy(buff.begin(), buff.end(), std::back_inserter(eval));
                tx->complete();
                return eval;
            }

            std::string &key() {
                return iter.key();
            }

            bool next() {
                ++iter;
                return iter != bt->end();
            }

            bool valid() {
                return iter != bt->end();
            }

            bool first() {
                iter = bt->begin();
                return iter != bt->end();
            }

            bool move(const std::string &key) {
                iter = bt->find(key);
                return iter != bt->end();
            }

            ~iterator() {

            }
        };

        std::shared_ptr<iterator> begin() const {
            return std::make_shared<iterator>(&data, &tx);
        }

        std::shared_ptr<iterator> lower_bound(const std::string &lb) const {
            return std::make_shared<iterator>(&data, &tx, lb);
        }
    };

    typedef BtDb _DbType;

    class GraphDB {
    public:
        /**
         * the identity type is required to be strictly orderable
         */
        typedef uint64_t _Identity;
        typedef uint64_t _Size;
        typedef uint64_t _NumberKey;
        typedef std::string _Key;
        typedef std::string _Value;

        enum {
            Internal,
            External,
            ExternalNumber // this causes numbered blocks to be put at the end of the fs (it has some advantages)
        };

        struct Node {

            uint32_t type = External;
            _Identity context{_Identity()};
            std::string name;

            Node() {};

            Node(uint32_t type, _Identity context, const std::string &name)
                    : type(type), context(context), name(name) {

            }

            void clear() {
                type = External;
                context = _Identity();
                name.clear();
            }

            const std::string &append_serialize(std::string &data) const {

                encode(data, type);
                encode(data, context);
                data.append(name);
                return data;
            }

            const std::string &serialize(std::string &data) const {
                data.clear();
                return append_serialize(data);
            }

            bool read(const std::string &value) {
                return read(value.data(), value.size());
            }

            bool read(const char *value, size_t size) {
                clear();
                if (size < sizeof(_Identity) + sizeof(type)) {
                    return false;
                }
                size_t offset = 0;
                offset += decode(type, value, size, offset);
                offset += decode(context, value, size, offset);
                name.clear();
                name.append(value + offset, size - offset);
                return true;
            }
        };

        struct NumberNode {
            uint32_t type = ExternalNumber;
            _Identity context{_Identity()};
            uint64_t number;

            NumberNode() {};

            NumberNode(uint32_t type, _Identity context, const uint64_t number)
                    : type(type), context(context), number(number) {

            }

            void clear() {
                type = ExternalNumber;
                context = _Identity();
                number = 0LL;
            }

            const std::string &append_serialize(std::string &data) const {
                encode(data, type);
                encode(data, context);
                encode(data, number);
                return data;
            }

            const std::string &serialize(std::string &data) const {
                data.clear();
                return append_serialize(data);
            }

            bool read(const std::string &value) {
                return read(value.data(), value.size());
            }

            bool read(const char *value, size_t size) {
                clear();
                if (size < sizeof(_Identity) + sizeof(type)) {
                    return false;
                }
                size_t offset = 0;
                offset += decode(type, value, size, offset);
                offset += decode(context, value, size, offset);
                offset += decode(number, value, size, offset);
                return true;
            }
        };

        struct StringData {
            _Identity id{_Identity()};
            std::string value;

            StringData() {}

            void clear() {
                id = _Identity();
                value = std::string();
            }

            const std::string &append_serialize(std::string &data) const {

                encode(data, id);
                data.append(value);
                return data;
            }

            const std::string &serialize(std::string &data) const {
                data.clear();
                return append_serialize(data);
            }

            bool read(const std::string &from) {
                return read(from.data(), from.size());
            }

            bool read(const char *from, size_t size) {
                clear();
                if (size < sizeof(_Identity)) {
                    return false;
                }
                size_t offset = 0;
                offset += decode(id, from, size, offset);
                value.clear();
                value.append(from + offset, size - offset);
                return true;
            }
        };

        struct IdData {
            _Identity id{_Identity()};
            _Identity value{_Identity()};

            void clear() {
                id = _Identity();
                value = _Identity();
            }

            const std::string &append_serialize(std::string &data) const {
                encode(data, id);
                encode(data, value);
                return data;
            }

            const std::string &serialize(std::string &data) const {
                data.clear();
                return append_serialize(data);
            }

            bool read(const std::string &data) {
                return read(data.data(), data.size());
            }

            bool read(const char *from, size_t size) {
                clear();
                if (size < sizeof(_Identity) * 2) {
                    return false;
                }
                size_t offset = 0;
                offset += decode(id, from, size, offset);
                offset += decode(value, from, size, offset);
                return true;
            }
        };

    private:
        struct per_thread {
            Node temp_node{External, 0, ""};
            NumberNode temp_number{ExternalNumber, 0, 0};
            StringData temp_data;

            std::string temp_value;
            std::string temp_value_data;
            std::string temp_node_data;
        };

        per_thread &get_per_thread() {
            thread_local per_thread r;
            return r;
        }

    private:
        std::mutex lock;

        std::string location;
        const Node id_node{Internal, 0, "id"};

        IdData id_value;
        std::string id_value_data;
        std::string id_node_data;
        _DbType db;

    private:

        void open() {
            if (!db.open(location)) {
                std::cerr << "could not open data store" << std::endl;
            }
        }

        bool close() {
            if (!db.is_open()) return false;
            return true;
        }

    public:

        GraphDB(std::string location) : location(location) {
            open();
        }

        ~GraphDB() {
            close();
        }

        bool opened() const {
            return db.is_open();
        }

        /**
         *
         * @return true if the graph is empty
         */
        bool is_new() {
            std::string id_node_data;
            std::string o;
            id_node.serialize(id_node_data);
            return !db.get(id_node_data, o);

        }

        /**
         * create a new identity - this function will never return the same value
         * Unless -
         *   the identity space is exhausted
         *   underlying data has been deleted
         * Note: this is a locking function
         * @return a unique monotonously increasing value
         */
        _Identity create() {

            if (!db.is_open()) return _Identity();
            std::unique_lock<std::mutex> _lock(lock);
            id_node_data.clear();
            id_node.serialize(id_node_data);
            std::string &temp_value = get_per_thread().temp_value;
            db.get(id_node_data, temp_value);
            // its ok if its not ok if(s.ok()){}
            id_value.read(temp_value);
            ++id_value.value;
            id_value.serialize(id_value_data);
            id_node.serialize(id_node_data);
            if (!db.put(id_node_data, id_value_data)) {
                return _Identity();
            }
            return id_value.value;
        }

        bool by_string(StringData &result, _Identity context, const _Key &name) {

            if (!db.is_open()) return false;
            auto &t = get_per_thread();
            auto &temp_node = t.temp_node;
            auto &temp_node_data = t.temp_node_data;
            auto &temp_value = t.temp_value;
            temp_node.name = name;
            temp_node.context = context;
            if (db.get(temp_node.serialize(temp_node_data), temp_value)) {
                return result.read(temp_value);
            }
            return false;

        }

        /**
         * return a raw value from a given context, name pair
         * many things can go wrong. like
         * * the context, name pair is not available
         * * the underlying data does not contain offset+size bytes
         * @param result data is copied into this pointer
         * @param size the byte count to copy
         * @param offset where in the source data to start copying
         * @param context number on the graph
         * @param name of the node under this context
         * @return true of operation could be completed
         */
        bool by_string_raw(char *result, size_t size, size_t offset, _Identity context, const _Key &name) {

            if (!db.is_open()) return false;
            auto &t = get_per_thread();
            auto &temp_node = t.temp_node;
            auto &temp_node_data = t.temp_node_data;
            temp_node.name = name;
            temp_node.context = context;
            return db.get(result, size, offset, temp_node.serialize(temp_node_data));

        }

        bool by_number(StringData &result, _Identity context, uint64_t number) {

            if (!db.is_open()) return false;
            auto &t = get_per_thread();
            auto &temp_number = t.temp_number;
            auto &temp_node_data = t.temp_node_data;
            auto &temp_value = t.temp_value;
            temp_number.number = number;
            temp_number.context = context;
            temp_value.clear();
            if (db.get(temp_number.serialize(temp_node_data), temp_value)) {
                return result.read(temp_value);
            }
            return false;

        }

        /**
        * return a raw value from a given context, name pair
        * many things can go wrong. like
        * * the context, number pair is not available
        * * the underlying data does not contain offset+size bytes
        * @param result data is copied into this pointer
        * @param size the byte count to copy
        * @param offset where in the source data to start copying
        * @param context number on the graph
        * @param number of the node under this context
        * @return true of operation could be completed
        */
        bool by_number_raw(char *result, size_t size, size_t offset, _Identity context, uint64_t number) {

            if (!db.is_open()) return false;
            auto &t = get_per_thread();
            auto &temp_number = t.temp_number;
            auto &temp_node_data = t.temp_node_data;
            temp_number.number = number;
            temp_number.context = context;

            return db.get(result, size, offset, temp_number.serialize(temp_node_data));

        }

        /**
         *
         * @param context
         * @param id
         * @param name
         * @param data
         * @return true if a new one has been added - if the relation already exists its not added and false is returned
         */
        bool add(_Identity context, _Identity id, const _Key &name, const _Value &data) {

            if (!db.is_open()) return false;
            auto &t = get_per_thread();
            auto &temp_node = t.temp_node;
            auto &temp_data = t.temp_data;
            auto &temp_node_data = t.temp_node_data;
            auto &temp_value_data = t.temp_value_data;
            temp_node.name = name;
            temp_node.context = context;
            temp_data.id = id;
            temp_data.value = data;
            return db.put(temp_node.serialize(temp_node_data), temp_data.serialize(temp_value_data));

        }

        bool
        add_raw(_Identity context, _Identity id, const _Key &name, const char *buf, size_t size, size_t intro_offset) {

            if (!db.is_open()) return false;
            auto &t = get_per_thread();
            auto &temp_node = t.temp_node;
            auto &temp_data = t.temp_data;
            auto &temp_node_data = t.temp_node_data;
            temp_node.name = name;
            temp_node.context = context;
            temp_data.id = id;

            return db.put(temp_node.serialize(temp_node_data), buf, size, intro_offset);

        }

        bool
        add_raw(_Identity context, _Identity id, uint64_t number, const char *buf, size_t size, size_t intro_offset) {

            if (!db.is_open()) return false;
            auto &t = get_per_thread();
            auto &temp_number = t.temp_number;
            auto &temp_data = t.temp_data;
            auto &temp_node_data = t.temp_node_data;
            temp_number.number = number;
            temp_number.context = context;
            temp_data.id = id;

            return db.put(temp_number.serialize(temp_node_data), buf, size, intro_offset);

        }

        bool remove(_Identity context, const _Key &name) {

            if (!db.is_open()) return false;
            auto &t = get_per_thread();
            auto &temp_node = t.temp_node;
            auto &temp_node_data = t.temp_node_data;
            temp_node.name = name;
            temp_node.context = context;
            return db.remove(temp_node.serialize(temp_node_data));

        }

        bool add(_Identity context, _Identity id, uint64_t number, const _Value &data) {

            if (!db.is_open())
                return false;
            auto &t = get_per_thread();
            auto &temp_number = t.temp_number;
            auto &temp_data = t.temp_data;
            auto &temp_node_data = t.temp_node_data;
            auto &temp_value_data = t.temp_value_data;
            temp_number.number = number;
            temp_number.context = context;
            temp_data.id = id;
            temp_data.value = data;
            return db.put(temp_number.serialize(temp_node_data), temp_data.serialize(temp_value_data));

        }

        const _Value &data(_Identity context, uint64_t number) {

            auto &t = get_per_thread();
            auto &temp_number = t.temp_number;
            auto &temp_data = t.temp_data;
            auto &temp_node_data = t.temp_node_data;
            auto &temp_value = t.temp_value;
            temp_data.clear();
            if (!db.is_open()) return temp_data.value;
            temp_number.number = number;
            temp_number.context = context;
            if (db.get(temp_number.serialize(temp_node_data), temp_value)) {
                temp_data.read(temp_value);
                return temp_data.value;
            }
            temp_data.clear();
            return temp_data.value;
        }

        bool remove(_Identity context, uint64_t number) {

            if (!db.is_open()) return false;
            auto &t = get_per_thread();
            auto &temp_number = t.temp_number;
            auto &temp_node_data = t.temp_node_data;

            temp_number.number = number;
            temp_number.context = context;
            return db.remove(temp_number.serialize(temp_node_data));
        }

        bool exists(_Identity context, uint64_t number) {

            if (!db.is_open()) return false;
            auto &t = get_per_thread();
            auto &temp_number = t.temp_number;
            auto &temp_node_data = t.temp_node_data;
            auto &temp_value = t.temp_value;

            temp_number.number = number;
            temp_number.context = context;
            
            return db.get(temp_number.serialize(temp_node_data), temp_value);
        }

        /**
         *
         * @param context
         * @param name
         * @param data
         * @return
         */
        bool add(_Identity context, const _Key &name, const _Value &data) {
            return add(context, create(), name, data);
        }


        bool exists(_Identity context, const _Key &name) {

            if (!db.is_open()) return false;
            auto &t = get_per_thread();
            auto &temp_node = t.temp_node;
            auto &temp_node_data = t.temp_node_data;
            auto &temp_value = t.temp_value;

            temp_node.name = name;
            temp_node.context = context;
            return db.get(temp_node.serialize(temp_node_data), temp_value);

        }

        template<typename _FunctionType>
        bool iterate(_Identity context, _FunctionType &&cb) {
            return iterate(context, "", cb);
        }

        struct prefix_string_iterator {

            std::shared_ptr<_DbType::iterator> i{nullptr};
            std::string prefix;
            std::string lb;
            Node node;
            std::string lb_data;

            void close() {
                i = nullptr;
            }

            bool move(const _DbType *db, _Identity context, const _Key &name) {
                close();
                node.name = name;
                node.context = context;
                lb = node.serialize(lb_data);
                prefix = lb;
                prefix.resize(prefix.size() - name.size());
                i = db->lower_bound(prefix);

                return valid();
            }

            prefix_string_iterator() {}

            prefix_string_iterator(const _DbType *db, _Identity context, const _Key name) {
                move(db, context, name);
            }

            prefix_string_iterator &operator=(const prefix_string_iterator &) = delete;

            prefix_string_iterator(const prefix_string_iterator &) = delete;

            ~prefix_string_iterator() {

            }

            bool first() {
                if (i)
                    i->first();
                return valid();
            }

            bool valid() {
                return i && i->valid() && (i->key().rfind(prefix, 0) == 0);
            }

            void next() {
                i->next();
            }

            bool read(Node &node) {
                if (valid()) {
                    return node.read(i->key());
                }
                return false;
            }

            bool read(StringData &data) {
                if (valid()) {
                    return data.read(i->value());
                }
                return false;
            }
        };

        struct prefix_number_iterator {

            std::shared_ptr<_DbType::iterator> i;
            NumberNode number;
            std::string prefix;
            std::string lb;
            std::string lb_data;

            void close() {
                i = nullptr;
            }

            bool move(const _DbType *db, _Identity context, const _NumberKey &n) {
                close();
                number.number = n;
                number.context = context;
                lb = number.serialize(lb_data);
                prefix = lb;
                prefix.resize(prefix.size() - sizeof(_NumberKey));
                i = db->lower_bound(prefix);

                return valid();
            }

            prefix_number_iterator() {};

            prefix_number_iterator(const _DbType *db, _Identity context, const _NumberKey &n) {
                move(db, context, n);
            }

            ~prefix_number_iterator() {
                close();
            }

            prefix_number_iterator(const prefix_number_iterator &) = delete;

            prefix_number_iterator operator=(const prefix_number_iterator &) = delete;

            bool first() {
                if (i)
                    i->first();
                return valid();
            }

            bool valid() {
                return i && i->valid() && (i->key().rfind(prefix, 0) == 0);
            }

            void next() {
                i->next();
            }

            bool read(NumberNode &number) {
                if (valid()) {
                    return number.read(i->key());
                }
                return false;
            }

            bool read(StringData &data) {
                if (valid()) {
                    return data.read(i->value());
                }
                return false;
            }

        };

        template<typename _FunctionType>
        bool iterate(_Identity context, const _Key &name, _FunctionType &&cb) {

            if (!db.is_open()) return false;
            auto &t = get_per_thread();
            prefix_string_iterator iter(&db, context, name);
            for (; iter.valid(); iter.next()) {
                auto kv = iter.i->key();
                auto iv = iter.i->value();
                uint32_t type;
                if (decode(type, kv.data(), kv.size(), 0) != sizeof(type)) {
                    // invalid key - stop iterating?
                    return false;
                }
                if (type == External) {
                    t.temp_data.read(iv);
                    t.temp_node.read(kv);
                    if (!cb(t.temp_node.name, t.temp_data.id, t.temp_data.value)) {
                        break;
                    }
                } else {
                    // something is seriously wrong I think?
                    return false;
                }

            }
            return true;

        }


        template<typename _FunctionType>
        bool iterate(_Identity context, const _NumberKey &number, _FunctionType &&cb) {

            if (!db.is_open()) return false;
            auto &t = get_per_thread();

            prefix_number_iterator iter(&db, context, number);

            for (; iter.valid(); iter.next()) {
                auto kv = iter.i->key();
                auto iv = iter.i->value();
                uint32_t type;
                if (decode(type, kv.data(), kv.size(), 0) != sizeof(type)) {
                    // invalid key - stop iterating?
                    return false;
                }
                if (type == ExternalNumber) {
                    t.temp_data.read(iv);
                    t.temp_number.read(kv);
                    if (!cb(t.temp_number.number, t.temp_data.id, t.temp_data.value)) {
                        break;
                    }
                } else {
                    // something is seriously wrong I think?
                    return false;
                }

            }
            return true;

        }

        bool move(prefix_string_iterator &p, _Identity context, const _Key name) const {
            return p.move(&db, context, name);
        }

        bool move(prefix_number_iterator &p, _Identity context, const _NumberKey number) const {
            return p.move(&db, context, number);
        }

        std::shared_ptr<prefix_string_iterator> strings(_Identity context, const _Key name) const {
            return std::make_shared<prefix_string_iterator>(&db, context, name);
        }

        std::shared_ptr<prefix_number_iterator> numbers(_Identity context, const _NumberKey &number) const {
            return std::make_shared<prefix_number_iterator>(&db, context, number);
        }

        /**
         * total used in underlying storage. can be more than actual bytes used
         * @return bytes allocated
         */
        _Size capacity();

        /**
         * a value less than or equal to capacity
         * @return bytes used
         */
        _Size used();

    };
};


#endif //REPLIFS_BT_GRAPHDB_H
