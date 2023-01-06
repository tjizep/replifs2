//
// Created by Pretorius, Christiaan on 2020-06-09.
//

#ifndef REPLIFS_ROCKSGRAPHDB_H
#define REPLIFS_ROCKSGRAPHDB_H

#include <mutex>
#include <string>
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include <iostream>

extern void test_btree();

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
        const uint32_t vsize = sizeof(_PrimitiveInteger);
        _PrimitiveInteger bits = vsize << 3;
        for (int i = 0; i < vsize; ++i) {
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
        const uint32_t vsize = sizeof(_PrimitiveInteger);
        if (offset + vsize > size) {
            std::cerr << "Decoding data from invalid string" << std::endl;
            return 0;
        }
        r = 0;
        const _PrimitiveInteger mask = 0xffL;
        _PrimitiveInteger bits = vsize << 3;
        for (int i = 0; i < vsize; ++i) {
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

            bool read(const rocksdb::Slice &from) {
                return read(from.data(), from.size());
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

            bool read(const rocksdb::Slice &from) {
                return read(from.data(), from.size());
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

            bool read(const rocksdb::Slice &from) {
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

            bool read(const rocksdb::Slice &from) {
                return read(from.data(), from.size());
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
        rocksdb::DB *db = nullptr;
        std::string location;
        const Node id_node{Internal, 0, "id"};

        IdData id_value;
        std::string id_value_data;
        std::string id_node_data;

    private:

        void open() {
            if (db != nullptr) close();
            using namespace rocksdb;
            Options options;
            options.compression = rocksdb::CompressionType::kNoCompression;
            options.create_if_missing = true;
            // Open DB
            Status s = DB::Open(options, location, &db);
            if (!s.ok()) {
                std::cerr << "Storage at " << location << " could not be opened " << std::endl;
                return;
            }

        }

        bool close() {
            if (db == nullptr) return false;
            using namespace rocksdb;
            return true;
        }

    public:

        GraphDB(std::string location) : location(location) {
            open();
        };

        ~GraphDB() {
            close();
        }

        bool opened() const {
            return (db != nullptr);
        }

        /**
         *
         * @return true if the graph is empty
         */
        bool is_new() {
            using namespace rocksdb;
            if (db == nullptr) return true;
            std::string id_node_data;
            std::string o;
            id_node.serialize(id_node_data);

            Status s = db->Get(ReadOptions(), id_node_data, &o);
            return (!s.ok());
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
            using namespace rocksdb;
            if (db == nullptr) return _Identity();
            std::unique_lock<std::mutex> _lock(lock);
            id_node_data.clear();
            id_node.serialize(id_node_data);
            std::string &temp_value = get_per_thread().temp_value;
            Status s = db->Get(ReadOptions(), id_node_data, &temp_value);
            // its ok if its not ok if(s.ok()){}
            id_value.read(temp_value);
            ++id_value.value;
            id_value.serialize(id_value_data);
            id_node.serialize(id_node_data);
            s = db->Put(WriteOptions(), id_node_data, id_value_data);
            if (!s.ok()) {
                return _Identity();
            }
            return id_value.value;
        }

        bool by_string(StringData &result, _Identity context, const _Key &name) {
            using namespace rocksdb;
            if (db == nullptr) return false;
            auto &t = get_per_thread();
            auto &temp_node = t.temp_node;
            auto &temp_node_data = t.temp_node_data;
            auto &temp_value = t.temp_value;
            temp_node.name = name;
            temp_node.context = context;
            Status s = db->Get(ReadOptions(), temp_node.serialize(temp_node_data), &temp_value);
            if (s.ok())
                return result.read(temp_value);

            return s.ok();
        }

        bool by_number(StringData &result, _Identity context, uint64_t number) {
            using namespace rocksdb;
            if (db == nullptr) return false;
            auto &t = get_per_thread();
            auto &temp_number = t.temp_number;
            auto &temp_node_data = t.temp_node_data;
            auto &temp_value = t.temp_value;
            temp_number.number = number;
            temp_number.context = context;
            temp_value.clear();
            Status s = db->Get(ReadOptions(), temp_number.serialize(temp_node_data), &temp_value);
            if (s.ok())
                return result.read(temp_value);

            return s.ok();
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
            using namespace rocksdb;
            if (db == nullptr) return false;
            auto &t = get_per_thread();
            auto &temp_node = t.temp_node;
            auto &temp_data = t.temp_data;
            auto &temp_node_data = t.temp_node_data;
            auto &temp_value_data = t.temp_value_data;
            temp_node.name = name;
            temp_node.context = context;
            temp_data.id = id;
            temp_data.value = data;

            Status s = db->Put(WriteOptions(), temp_node.serialize(temp_node_data),
                               temp_data.serialize(temp_value_data));
            return s.ok();
        }

        bool remove(_Identity context, const _Key &name) {
            using namespace rocksdb;
            if (db == nullptr) return false;
            auto &t = get_per_thread();
            auto &temp_node = t.temp_node;
            auto &temp_data = t.temp_data;
            auto &temp_node_data = t.temp_node_data;
            auto &temp_value_data = t.temp_value_data;
            temp_node.name = name;
            temp_node.context = context;
            Status s = db->Delete(WriteOptions(), temp_node.serialize(temp_node_data));
            return s.ok();
        }

        bool add(_Identity context, _Identity id, uint64_t number, const _Value &data) {
            using namespace rocksdb;
            if (db == nullptr)
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
            Status s = db->Put(WriteOptions(), temp_number.serialize(temp_node_data),
                               temp_data.serialize(temp_value_data));
            return s.ok();
        }

        const _Value &data(_Identity context, uint64_t number) {
            using namespace rocksdb;
            auto &t = get_per_thread();
            auto &temp_number = t.temp_number;
            auto &temp_data = t.temp_data;
            auto &temp_node_data = t.temp_node_data;
            auto &temp_value = t.temp_value;
            temp_data.clear();
            if (db == nullptr) return temp_data.value;
            temp_number.number = number;
            temp_number.context = context;
            Status s = db->Get(ReadOptions(), temp_number.serialize(temp_node_data), &temp_value);
            if (s.ok()) {
                temp_data.read(temp_value);
                return temp_data.value;
            }
            temp_data.clear();
            return temp_data.value;
        }

        bool remove(_Identity context, uint64_t number) {
            using namespace rocksdb;
            if (db == nullptr) return false;
            auto &t = get_per_thread();
            auto &temp_number = t.temp_number;
            auto &temp_node_data = t.temp_node_data;

            temp_number.number = number;
            temp_number.context = context;
            Status s = db->Delete(WriteOptions(), temp_number.serialize(temp_node_data));
            return s.ok();
        }

        bool exists(_Identity context, uint64_t number) {
            using namespace rocksdb;
            if (db == nullptr) return false;
            auto &t = get_per_thread();
            auto &temp_number = t.temp_number;
            auto &temp_node_data = t.temp_node_data;
            auto &temp_value = t.temp_value;

            temp_number.number = number;
            temp_number.context = context;
            bool ex = false;
            if (db->KeyMayExist(ReadOptions(), temp_number.serialize(temp_node_data), &temp_value, &ex)) {
                return ex;
            }

            return false;
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
            using namespace rocksdb;
            if (db == nullptr) return false;
            auto &t = get_per_thread();
            auto &temp_node = t.temp_node;
            auto &temp_node_data = t.temp_node_data;
            auto &temp_value = t.temp_value;

            temp_node.name = name;
            temp_node.context = context;
            bool ex = false;
            if (db->KeyMayExist(ReadOptions(), temp_node.serialize(temp_node_data), &temp_value, &ex)) {
                return ex;
            }

            return false;
        }

        template<typename _FunctionType>
        bool iterate(_Identity context, _FunctionType &&cb) {
            return iterate(context, "", cb);
        }

        struct prefix_string_iterator {

            rocksdb::Iterator *i{nullptr};
            Node node;
            rocksdb::Slice prefix;
            rocksdb::Slice lb;
            std::string lb_data;

            void close() {
                if (i != nullptr) {
                    delete i;
                    i = nullptr;
                }
            }

            bool move(rocksdb::DB *db, _Identity context, const _Key &name) {
                close();
                using namespace rocksdb;
                rocksdb::ReadOptions read_options;
                node.name = name;
                node.context = context;
                lb = node.serialize(lb_data);
                prefix = lb;
                //read_options.auto_prefix_mode = true;
                read_options.iterate_lower_bound = &lb;
                prefix.remove_suffix(name.size());
                i = db->NewIterator(read_options); /// a really shitty api
                if (!i->status().ok()) {
                    std::cerr << "Iterator error " << i->status().ToString() << std::endl;
                }
                return first();
            }

            prefix_string_iterator() {}

            prefix_string_iterator(rocksdb::DB *db, _Identity context, const _Key name) {
                move(db, context, name);
            }

            prefix_string_iterator &operator=(const prefix_string_iterator &) = delete;

            prefix_string_iterator(const prefix_string_iterator &) = delete;

            ~prefix_string_iterator() {
                if (i)
                    delete i;
            }

            bool first() {
                if (i)
                    i->SeekToFirst();
                return valid();
            }

            bool valid() {
                return i && i->status().ok() && i->Valid() && i->key().starts_with(prefix);
            }

            void next() {
                i->Next();
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

            rocksdb::Iterator *i{nullptr};
            NumberNode number;
            rocksdb::Slice prefix;
            rocksdb::Slice lb;
            std::string lb_data;

            void close() {
                if (i)
                    delete i;
            }

            bool move(rocksdb::DB *db, _Identity context, const _NumberKey &n) {
                close();
                using namespace rocksdb;
                rocksdb::ReadOptions read_options;
                number.number = n;
                number.context = context;
                lb = number.serialize(lb_data);
                prefix = lb;
                //read_options.auto_prefix_mode = true;
                read_options.iterate_lower_bound = &lb; // to constrain iteration to context only
                i = db->NewIterator(read_options);
                prefix.remove_suffix(sizeof(_NumberKey));
                return first();
            }

            prefix_number_iterator() {};

            prefix_number_iterator(rocksdb::DB *db, _Identity context, const _NumberKey &n) {
                move(db, context, n);
            }

            ~prefix_number_iterator() {
                close();
            }

            prefix_number_iterator(const prefix_number_iterator &) = delete;

            prefix_number_iterator operator=(const prefix_number_iterator &) = delete;

            bool first() {
                if (i)
                    i->SeekToFirst();
                return valid();
            }

            bool valid() {
                return i && i->status().ok() && i->Valid() && i->key().starts_with(prefix);
            }

            void next() {
                i->Next();
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
            using namespace rocksdb;
            if (db == nullptr) return false;
            auto &t = get_per_thread();
            prefix_string_iterator iter(db, context, name);
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
            using namespace rocksdb;
            if (db == nullptr) return false;
            auto &t = get_per_thread();

            prefix_number_iterator iter(db, context, number);

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
            return p.move(db, context, name);
        }

        bool move(prefix_number_iterator &p, _Identity context, const _NumberKey number) const {
            return p.move(db, context, number);
        }

        std::shared_ptr<prefix_string_iterator> strings(_Identity context, const _Key name) const {
            return std::make_shared<prefix_string_iterator>(db, context, name);
        }

        std::shared_ptr<prefix_number_iterator> numbers(_Identity context, const _NumberKey &number) const {
            return std::make_shared<prefix_number_iterator>(db, context, number);
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


#endif //REPLIFS_ROCKSGRAPHDB_H
