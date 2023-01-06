//
// Created by Pretorius, Christiaan on 2020-07-04.
//

#ifndef REPLIFS_TRANSACTION_H
#define REPLIFS_TRANSACTION_H

#include "file_storage_alloc.h"
//#include "rabbit/unordered_map"
namespace persist {
    namespace storage {
        /**
         * transaction class representing a transaction to callers of persistent/consistent
         * storages
         * call begin to start a transaction
         * then rollback or commit when a set of changes represents a consistent state
         */
        class transaction {
        private:
            const Constants constants;
            //TODO: make these sizes configurable and or automatic
            lru_cache<u64, std::shared_ptr<buffer_type>> data_cache{10000};
            // write buffer to speedup multiple writes to the same buffer
            lru_cache<u64, std::shared_ptr<buffer_type>> write_buffer{2000};
            // only data read from other transactions is stored here
            std::unordered_map<u64, AllocationRecord> read_allocation_map;
            // only data allocated in *this* transaction is stored here
            std::unordered_map<u64, AllocationRecord> allocation_map;
            // only boot record changes in *this* transaction is recorded here
            std::unordered_map<u64, u64> boot_map;
            //** prevents lru from releasing page during allocation and improves perf.
            std::shared_ptr<buffer_type> current;
            // current allocation record on existing pages - for read and update actions
            AllocationRecord current_ar{0, 0};
            // create,update or read
            i32 current_action{0};
            // a value retrieved from underlying file storage representing data logically
            // so that data can be remapped onto working set when transaction commits
            u64 current_logical{0};
            // statistic recording changes in allocation size maybe 0 even if data has been written to
            u64 bytes_allocated{0};
            // non 0 indicates errors
            u64 error_count{0};
            // the transaction id on which the current transaction is based - they should increase when a new tx starts
            types::version_id source_txid{0};
            // indicates the end of storage or an error happened during page allocation
            // to callers
            std::shared_ptr<buffer_type> end_buffer = std::make_shared<buffer_type>();
            // the version of this transaction - any changed records will use this id
            types::version_id version_id{0};
            // the underlying storage allocation
            file_storage_alloc *fa{nullptr};
            
            std::pair<bool, u64> get_int_boot_value(u64 k) const {
                if (error_count) return {false, u64()};
                if (fa == nullptr) return {false, u64()};
                auto f = get_int_boot(k);
                if (f.first) {
                    return {true, f.second};
                }
                return fa->get_int_boot_value(*this, k);
            }

            bool set_int_boot_value(u64 k, u64 val) {
                if (error_count) return false;
                if (k >= constants.MAX_BOOT_KEY) {
                    print_err("the boot key supplied", k, " exceeds the maximum ", constants.MAX_BOOT_KEY);
                    ++error_count;
                    return false;
                }
                this->set_int_boot(k, val);
                return true;
            }
            // allocate some new space and only record the new allocation in this transaction
            void flush_buffer(const u64 &logical, const std::shared_ptr<buffer_type> &buff){
                auto ar = fa->allocate_data(logical, *buff);
                if (!ar.empty()) {
                    write_allocation_record(ar, logical);
                } else {
                    print_err("could not allocate data after write");
                }
            }
        public:
            transaction() : fa(nullptr) {}

            transaction(file_storage_alloc *fa) : fa(fa) {
                begin();
            }

            void set_storage(file_storage_alloc *fa) {
                this->fa = fa;
            }

            void clear() {
                allocation_map.clear();
                boot_map.clear();
                write_buffer.clear();
                data_cache.clear();
                source_txid = 0;
            }

            bool set_alloc(AllocationRecord current, u64 l) {
                print_dbg(current.to_string(), l);
                allocation_map[l] = current;
                return true;
            }
            /**
             * write the allocation record to the current transaction using the current transactions version
             * as the new version
             */
            bool write_allocation_record(AllocationRecord current, u64 l) {
                if (error_count) return false;
                current.version = this->get_version_id();
                print_dbg("write allocation record in tx {", current.size, current.position,"} at", current.position, l);
                set_alloc(current, l);
                return true;
            }

            std::pair<bool, AllocationRecord> get_tx_alloc(u64 l) {
                if (!source_txid){
                    print_err("transaction has no source tx");
                    return {false, {0, 0}};  
                } 
                if (fa == nullptr){
                    print_err("transaction has no source allocator");
                    return {false, {0, 0}};  
                } 
                auto f = allocation_map.find(l);
                if (f != allocation_map.end()) {
                    return {true, f->second};
                }
                auto result = fa->get_alloc(*this, l);
                if(result.first){
                    read_allocation_map[l] = result.second;  
                }
                return result;
            }

            AllocationRecord get_alloc(u64 l) {
                print_dbg("load allocation record at", l);
                auto ar = get_tx_alloc(l);
                if (ar.first) {
                    return ar.second;
                }
                return {0, 0};
            }

            void set_int_boot(u64 k, u64 val) {
                print_dbg("set int boot", k, val);
                boot_map[k] = val;
            }

            std::pair<bool, u64> get_int_boot(u64 k) const {
                
                auto f = boot_map.find(k);
                if (f != boot_map.end()) {
                    print_dbg("get_int_boot",k,f->second);
                    return std::make_pair(true, f->second);
                }
                print_dbg("get_int_boot",k,"u64()");
                return std::make_pair(false, u64());
            }

            template<typename _Ft>
            void iter_write_buf(_Ft &&cb) {
                print_dbg("iterating write buffers");
                for (auto b:write_buffer) {
                    cb(b.first, b.second);
                }
            }

            template<typename _Ft>
            void iter_boot(_Ft &&cb) {
                print_dbg("iterating boot map",boot_map.size());
                for (auto b: boot_map) {
                    cb(b.first, b.second);
                }
            }

            // the contract os to iterate on modified blocks/values only
            template<typename _Ft>
            void iter_alloc(_Ft &&cb) {
                print_dbg(allocation_map.size());
                for (auto a: allocation_map) {
                    auto ar = read_allocation_map.find(a.first);
                    print_dbg(a.first, a.second.to_string(), allocation_map.size(),(ar != read_allocation_map.end()));
                    cb(a.first, a.second, (ar != read_allocation_map.end()) ? ar->second : AllocationRecord(0,0));
                }
            }

            void set_boot_value(i64 val, u64 index) {
                print_dbg("set_boot_value",val,index);
                set_int_boot_value(index + constants.EXTERNAL_BOOT_VAL_START, val);
            }

            bool get_boot_value(u64 &val, u64 index) const {
                auto rval = get_int_boot_value(index + constants.EXTERNAL_BOOT_VAL_START);
                val = rval.second;
                return rval.first;
            }

            std::string get_name() const {
                if (fa == nullptr) return "";
                return fa->get_name();
            }

            bool is_readonly() const {
                return false;
            }

            version_type get_version() const {
                return {0};
            }

            version_type get_allocated_version() const {
                return {0};
            }

            u64 current_transaction_order() {
                return 1;
            }

            buffer_type &allocate(u64 &address, storage_action action) {
                if (fa == nullptr) {
                    print_err("transaction not attached");
                    return *end_buffer;
                }
                if (!source_txid) {
                    print_err("transaction not started");
                    return *end_buffer;
                }
                print_dbg("address", address);
                if (error_count) return *end_buffer;

                std::shared_ptr<buffer_type> r;

                if (address == 0 && action == create) {
                    address = fa->new_logical();
                    if (!address) {
                        r = end_buffer;
                    } else {
                        r = std::make_shared<buffer_type>();
                        //data_cache.insert(address,r);
                    }
                } else { // if action != create
                    auto wvi = write_buffer.find(address); // check the write buffer first
                    if (wvi.first) {
                        r = wvi.second;
                    } else {
                        auto dvi = data_cache.find(address);
                        if (!dvi.first) {
                            auto ar = get_alloc(address);
                            if (ar.empty()) {
                                r = end_buffer; // nothing found
                            } else {

                                r = std::make_shared<buffer_type>();
                                r->resize(ar.size);
                                if (!fa->read_vec_at(*r, ar.position,
                                                     "read for data")) { // read data from storage into buffer
                                    r = end_buffer;
                                }
                            }
                        } else {
                            r = dvi.second;
                        }
                    }
                }
                current_logical = address;
                current = r;
                current_action = action;

                bytes_allocated -= current->size();

                return *r;
            }

            // this function will write the modified buffer into storage - the storage is
            // responsible for allocating this buffer
            // if allocate is called before this function then the changes are lost
            void complete() {
                if (fa == nullptr) {
                    print_err("transaction not attached");
                    return;
                }
                bytes_allocated += current->size();

                if (current_action != storage_action::read) {// write modified buffer back into storage
                    write_buffer.insert(current_logical, current,
                        // if the write buffer is full this function will be called on eviction
                        [&](const u64 &logical, const std::shared_ptr<buffer_type> &buff) {
                            flush_buffer(logical, buff);
                        }
                    );
                }
            }

            size_t store_size(const std::string &s) const {
                return s.size() + sizeof(u32);
            }

            size_t store_size(u64 /*s*/) const {
                return sizeof(u64);
            }

            buffer_type::iterator
            store
            (   buffer_type::iterator writer, 
                buffer_type::iterator /*limit*/, // TODO: use for safety check 
                const std::string &data
            ) {
                u32 dl = data.size();

                writer = primitive::store(writer, dl);
#if DEBUG
                buffer_type::iterator old_writer = writer;
#endif
                writer = std::copy(data.begin(), data.end(), writer);
#if DEBUG
                size_t dw = writer - old_writer;
                assert(dw == dl);
#endif
                return writer;
            }

            buffer_type::iterator
            store
            (   buffer_type::iterator writer, 
                buffer_type::iterator /*limit*/, 
                u64 data
            ) {
                return nst::primitive::store(writer, data);
            }

            buffer_type::const_iterator
            retrieve
            (   const buffer_type &/*buffer*/, /// TODO: check buffer end - since its here 
                buffer_type::const_iterator reader, 
                u64 &data
            ) {

                return primitive::read(data, reader);
            }

            buffer_type::const_iterator
            retrieve
            (   const buffer_type &/*buffer*/, ///TODO: check buffer end - since its here
                buffer_type::const_iterator reader, 
                std::string &key
            ) {
                u32 dl{0};
                auto r = primitive::read(dl, reader);
                auto e = r + dl;
                key.clear();
                for (; r != e; ++r) {
                    key.push_back(*r);
                }
                return r;

            }

            bool is_end(const buffer_type &b) {
                return end_buffer.get() == &b;
            }

            types::version_id get_source_txid() const {
                return source_txid;
            }
            void set_version_id(types::version_id version_id) {
                this->version_id = version_id;
            }
            types::version_id get_version_id() const {
                return this->version_id;
            }
            bool set_source_txid(types::version_id txid) {
                if (txid <= this->source_txid) {
                    print_err("cannot set tx id not more than current source_txid");
                    return false;
                }
                this->source_txid = txid;
                return true;
            }

            bool begin() {
                if (fa == nullptr) {
                    print_err("transaction not attached");
                    return false;
                }
                if (source_txid) {
                    print_err("transaction already started");
                    return false;
                }
                // start with empty buffers (TODO: optimize later, maybe)
                write_buffer.clear();
                data_cache.clear();
                read_allocation_map.clear();
                fa->begin(*this);
                return true;
            }

            bool rollback() {
                if (fa == nullptr) {
                    print_err("transaction not attached");
                    return false;
                }
                if (!source_txid) {
                    print_err("no transaction started");
                    return false;
                }
                // buffers are invalid now
                write_buffer.clear();
                data_cache.clear();
                read_allocation_map.clear();
                return fa->rollback(*this);
            }

            bool commit() {
                if (fa == nullptr) {
                    print_err("transaction not attached");
                    return false;
                }
                if (!source_txid) {
                    print_err("no transaction started");
                    return false;
                }
                print_dbg("commit", source_txid);
                write_buffer.flush(
                    // if the write buffer is full this function will be called on eviction
                    [&](const u64 &logical, const std::shared_ptr<buffer_type> &buff) {
                        flush_buffer(logical, buff);
                    }
                );
                bool r = fa->commit(*this);
                write_buffer.clear(); // buffers have been delivered (they will spoil the next transaction)
                data_cache.clear(); // these buffers may be spoilt in a multi user environment
                read_allocation_map.clear(); // start over now
                return r;
            }
        };
    }

}

#endif //REPLIFS_TRANSACTION_H
