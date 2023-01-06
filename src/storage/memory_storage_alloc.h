//
// Created by Pretorius, Christiaan on 2020-06-28.
//

#ifndef REPLIFS_MEMORY_STORAGE_ALLOC_H
#define REPLIFS_MEMORY_STORAGE_ALLOC_H

#include <persist/btree_map>
#include <unordered_map>
//#include <rabbit/unordered_map>

//#include <rabbit/unordered_map>
class memory_storage_alloc : persist::storage::basic_storage {

public:
    nst::i64 bytes_allocated{0};
    std::unordered_map<nst::u64, nst::u64> boot_values;
    std::unordered_map<nst::u64, std::shared_ptr<nst::buffer_type>> data_values;
    nst::u64 next_address{32};

    memory_storage_alloc() {
    }

    void set_boot_value(nst::i64 val, nst::u64 index) {
        boot_values[index] = val;
    }

    bool has_boot_value(nst::u64 index) const {
        return boot_values.count(index);
    }

    bool get_boot_value(nst::u64 &val, nst::u64 index) const {
        auto r = boot_values.find(index);
        if (r != boot_values.end()) {
            val = r->second;
            return true;
        }
        return false;
    }

    std::string get_name() const {
        return "default";
    }

    bool is_readonly() const {
        return false;
    }

    nst::version_type get_version() const {
        return {0};
    }

    nst::version_type get_allocated_version() const {
        return {0};
    }

    nst::u64 current_transaction_order() {
        return 1;
    }

    std::shared_ptr<nst::buffer_type> end_buffer = std::make_shared<nst::buffer_type>();
    std::shared_ptr<nst::buffer_type> current;

    nst::buffer_type &allocate(nst::u64 &address, nst::storage_action action) {
        std::shared_ptr<nst::buffer_type> r;

        if (address == 0 && action == nst::create) {
            address = ++next_address;
            r = std::make_shared<nst::buffer_type>();
            data_values[address] = r;
        } else {
            auto dvi = data_values.find(address);
            if (dvi == data_values.end()) {
                r = end_buffer;
            } else {
                r = dvi->second;
            }

        }
        current = r;
        bytes_allocated -= current->size();
        return *r;
    }

    void complete() {
        bytes_allocated += current->size();
    }

    size_t store_size(const std::string &s) const {
        return s.size() + sizeof(nst::u32);
    }

    size_t store_size(nst::u64) const {
        return sizeof(nst::u64);
    }

    nst::buffer_type::iterator
    store
    (   nst::buffer_type::iterator writer,
        nst::buffer_type::iterator /*limit*/,
        nst::u64 data
    ) {
        return nst::primitive::store(writer, data);
    }

    nst::buffer_type::const_iterator
    retrieve
    (   const nst::buffer_type &/*buffer*/, /// TODO: check buffer end - since its here 
        nst::buffer_type::const_iterator reader,
        nst::u64 &data
    ) {

        return nst::primitive::read(data, reader);
    }
    
    nst::buffer_type::iterator
    store(nst::buffer_type::iterator writer, nst::buffer_type::iterator /*limit*/, const std::string &data) {
        nst::u32 dl = data.size();

        writer = nst::primitive::store(writer, dl);

        writer = std::copy(data.begin(), data.end(), writer);

        return writer;
    }

    nst::buffer_type::const_iterator
    retrieve(const nst::buffer_type &/*buffer*/, nst::buffer_type::const_iterator reader, std::string &key) {
        nst::u32 dl{0};
        auto r = nst::primitive::read(dl, reader);
        key.clear();
        key.resize(dl);
        memcpy(&key[0], (const char *) &(*r), dl);
        r += dl;
        return r;

    }

    bool is_end(const nst::buffer_type &b) {
        return end_buffer.get() == &b;
    }

};

#endif //REPLIFS_MEMORY_STORAGE_ALLOC_H
