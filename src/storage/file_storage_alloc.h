//
// Created by Pretorius, Christiaan on 2020-06-30.
//

#ifndef REPLIFS_FILE_STORAGE_ALLOC_H
#define REPLIFS_FILE_STORAGE_ALLOC_H

#include "structured_file.h"
#include "lru_cache.h"
#include <set>

namespace persist {
    namespace storage {
        
        struct Constants {
            /// boot and allocation table layout start
            const u64 MAX_BOOT_KEY = 64;
            const u64 EXTERNAL_BOOT_VAL_START = 32;

            const u64 MAX_LOGICAL_ADDRESS = 10000000;
            const u64 FALLOC_VERSION = 0x80000002L;
            const u64 VERSION_SIZE = 4 * sizeof(u64);
            const u64 VERSION_START = 0ull;
            // computed header position list
            const u64 BOOT_TABLE_START = VERSION_START + VERSION_SIZE;
            const u64 BOOT_TABLE_SIZE = MAX_BOOT_KEY * sizeof(u64);
            const u64 ALLOC_TABLE_START = BOOT_TABLE_START + BOOT_TABLE_SIZE;
            const u64 ALLOC_TABLE_SIZE = MAX_LOGICAL_ADDRESS * sizeof(AllocationRecord);

            const u64 HEADER_START = VERSION_START;
            const u64 HEADER_SIZE = ALLOC_TABLE_START + ALLOC_TABLE_SIZE;
            /// boot and allocation table layout end

            // there can be a maximum of 16 concurrent versions active
            // more versions will
            const u64 MAX_VERSION = 16;

        };


        /**
         * this mvcc class is designed with the following objectives
         * 1. do not become inconsistent when storage runs out
         * 2. Perform well - write a minimum size WAL file
         * 3. copy on write semantics
         * 4. reduce memory copies when data is allocated
         * 6. Exist within a single file
         *
         * Limitations of this class is that all allocated addresses must fit in memory
         * worst case scenario is at 1k per block and 1 transaction writing to all blocks
         * in 128 TB we will need (8*128*1024*1024*1024*1024)/1024 = 1 TB of RAM per transaction
         */
        class file_storage_alloc : persist::storage::basic_storage, public structured_file {
        private:
            enum {
                Logical_Index = 4
            };

            // a version is kind of like a transaction - except its comitted already but unmerged
            struct version {
                // only data allocated in *this* transaction is stored here
                std::unordered_map<u64, AllocationRecord> allocation_map;
                // only boot record changes in *this* transaction is recorded here
                std::unordered_map<u64, u64> boot_map;
                // the locks held on this version by external transactions
                // when locks reaches 0 the version can be erased or merged with one version
                // down depending on its position in the versions list
                u64 locks{0};
                types::version_id txid{0};
                /**
                 * locks and return the version of the transaction
                 * @return the version id of this transaction
                 */
                types::version_id lock_txid() {
                    ++locks;
                    print_dbg("locks",locks);
                    return txid;
                }
                void unlock_txid() {
                    --locks;
                    print_dbg("locks",locks);
                }
            };

            typedef std::list<version> _VersionList;
            typedef _VersionList::iterator _VersionPtr;
            typedef std::unordered_map<types::version_id, _VersionPtr> _VersionMap;

            const Constants constants;

            mutable u64 error_count{0};
            // the free pages that can be reused for other transactions and data
            std::set<AllocationRecord> free_map;
            _VersionMap version_map;
            _VersionList version_list;

            u64 file_size{0};
#if 0 // for later
            i64 bytes_allocated{0};
#endif
            types::version_id txid_generator{0};
            std::string name;
            
            bool doverifyallocations{false};

            _VersionPtr latest_version() {
                return version_list.begin();
            }

            bool remove_version(types::version_id txid) {
                print_dbg("source_txid ",txid);
                auto f = version_map.find(txid);
                if (f != version_map.end()) {
                    _VersionPtr v = f->second;
                    version_map.erase(f);
                    version_list.erase(v);
                    print_dbg("ok source_txid",txid);
                    return true;
                }
                print_wrn("missing source_txid",txid);
                return false;
            }

            // this function cannot fail
            _VersionPtr create_version(types::version_id from_tx) {
                version_list.push_front(version());
                _VersionPtr v = version_list.begin();
                v->txid = from_tx;
                print_dbg("v->source_txid ",v->txid);
                auto old_size = version_map.size();
                version_map[v->txid] = v;
                if (old_size == version_map.size()) {
                    error_count++;
                    print_err("version id is not unique");
                }
                return v;
            }

            bool unlock_version(u64 txid) {
                print_dbg("source_txid",txid);
                auto v = version_map.find(txid);
                if (v != version_map.end()) {
                    print_dbg("locks",v->second->locks);
                    v->second->unlock_txid();
                    print_dbg("locks after",v->second->locks);
                    return true;
                }
                print_err("cannot unlock version which does not exist");
                return false;
            }

            bool commit_int_boot_value(u64 k, u64 val) {
                if (error_count) return false;
                if (k >= constants.MAX_BOOT_KEY) {
                    print_err("int boot value key exceeds maximum key value");
                    ++error_count;
                    return false;
                }
                print_dbg("k",k,"val",val);
                seek(constants.BOOT_TABLE_START + k * sizeof(u64), "int boot table");
                u64 vval = val + 1;
                return set(vval);
            }

            bool commit_allocation_record(AllocationRecord current, u64 l) {
                if (error_count) return false;
                print_dbg("commit allocation record in file {", current.size, current.position, current.version, "} at", l);
                seek(constants.ALLOC_TABLE_START + sizeof(AllocationRecord) * l, "write alloc rec.");
                if (set(current)) {
                    if (doverifyallocations) {
                        AllocationRecord test = get_alloc(l);
                        if (test != current) {
                            print_err("alloc verify failed");
                            return false;
                        }
                    }
                    return true;
                };
                return false;
            }

            bool scan_allocation_map() {
                if (!seek(constants.ALLOC_TABLE_START, "alloc table")) {
                    return false;
                }
                auto alloc = get<AllocationRecord>();
                u64 logical = 1;
                while (!alloc.empty() && error_count == 0 && logical < constants.MAX_LOGICAL_ADDRESS) {
                    if (logical == constants.MAX_LOGICAL_ADDRESS) { // do not attempt to read more
                        wrn_print("File data allocation table is full");
                        break;
                    }
                    alloc = get<AllocationRecord>();
                }
                return error_count == 0;
            }

            std::pair<bool, u64> get_int_boot_value(u64 k) const {
                if (error_count) return {false, u64()};
                seek(constants.BOOT_TABLE_START + k * sizeof(u64), "int boot table");
                u64 r = get<u64>();
                if (!r) {
                    return {false, r};
                }
                --r;
                return {true, r};
            }

            AllocationRecord get_alloc(u64 l) {
                print_dbg("load allocation record at", l);
                seek(constants.ALLOC_TABLE_START + sizeof(AllocationRecord) * l, "read alloc rec.");
                return get<AllocationRecord>();
            }

        public:
            // TODO: memory storage when no name is given
            file_storage_alloc() {
                create_version(++txid_generator);// the latest version
            }

            file_storage_alloc(const std::string &name) {
                create_version(++txid_generator);// the latest version
                open(name);
            }

            ~file_storage_alloc() {
                if (!name.empty()) {
                    synch();
                }
            }

            bool open(const std::string &file_name) {
                close();
                if (!exists(file_name) && !create_file(file_name)) {
                    print_err("could not find nor create '%s'", file_name.c_str());
                    return false;
                }
                if (!structured_file::open_file(file_name)) {
                    return check_error("open");
                }

                name = file_name;
                to_end();
                file_size = tell();
                seek(constants.VERSION_START, "open (version)");

                if (file_size >= constants.HEADER_START + constants.HEADER_SIZE) {
                    seek(constants.VERSION_START, "open (version)");
                    if ((get<u64>() & 0x7FFFFFFF) == constants.FALLOC_VERSION) {
                        print_err("invalid alloc file version");
                        close();
                        return false;
                    }
                    if (!(scan_allocation_map())) {
                        print_err("invalid alloc map");
                        return false;
                    }
                }

                if (file_size == 0) {
                    seek(constants.HEADER_START, "open ( init header )");
                    zero(constants.HEADER_SIZE);
                    seek(constants.VERSION_START, "open ( init version )");
                    set(constants.FALLOC_VERSION);
                    commit_int_boot_value(Logical_Index, constants.MAX_BOOT_KEY + 1);
                    synch();
                    to_end();
                    file_size = tell();
                    // resize file
                    print_dbg("opened file '",file_name,"' with size",file_size);
                } else if (file_size < constants.HEADER_START + constants.HEADER_SIZE) {
                    ++error_count;
                    print_err("invalid alloc init record");
                    close();
                    return false;
                }

                return true;
            }

            bool is_open() {
                return (file_size >= (constants.HEADER_START + constants.HEADER_SIZE) && error_count == 0);
            }

            u64 size() const {
                return file_size;
            }

            u64 new_logical() {
                u64 logical = get_int_boot_value(Logical_Index).second;
                if (logical <= constants.MAX_BOOT_KEY) {
                    ++error_count;
                    print_err("max logical address reached");
                    return u64();
                }
                if (logical >= constants.MAX_LOGICAL_ADDRESS) {
                    ++error_count;
                    print_err("max logical address reached");
                    return u64();
                }
                ++logical;
                commit_int_boot_value(Logical_Index, logical);
                return logical;
            }

            AllocationRecord find_free_allocation(u64 size) {
                if (free_map.empty()) return {0, 0};
                AllocationRecord lsize{size, 0};
                AllocationRecord result{size, 0};
                auto l = free_map.lower_bound(lsize);
                if (l != free_map.end()) {
                    u64 r = l->size - size;
                    AllocationRecord remaining{r, l->position + r};
                    result.position = l->position;
                    free_map.erase(l);
                    if (r > 32) {
                        free_map.insert(remaining);
                    }
                }
                return result;
            }
            // TODO: the logical parameter should be used or removed
            AllocationRecord allocate_data(u64 /*logical*/, const buffer_type &data) {
                if (file_size < constants.HEADER_START + constants.HEADER_SIZE) {
                    print_dbg("error allocating",data.size(),"bytes at", file_size);
                    ++error_count;
                    return {0, 0};
                }
                print_dbg("allocating",data.size(),"bytes at",file_size);
                AllocationRecord falloc = find_free_allocation(data.size());
                if (!falloc.empty()) {
                    seek(falloc.position, "re-alloc data");
                    if (!structured_file::write(data)) {
                        return {0, 0};
                    }
                    return falloc;
                }
                if (tell() != file_size)
                    seek(file_size, "alloc data");
                if (!structured_file::write(data)) {
                    return {0, 0};
                }
                u64 new_file_size = file_size + data.size(); //tell();
                if (error_count) {
                    print_dbg("error allocating",data.size(),"bytes", file_size);
                    return {0, 0};
                }
                AllocationRecord result{data.size(), file_size};

                file_size = new_file_size;
                print_dbg("allocated",data.size(),"bytes (",result.position,"actual) at", file_size);
                return result;
            }
            /**
             * get a boot value thats valid within the given transaction
             * @tparam _Transaction 
             * @param tx 
             * @param k the logical address of the boot record must be less than bootsector size
             * @return false if it does not exist or any pending errors are active
             */
            template<typename _Transaction>
            std::pair<bool, u64> get_int_boot_value(const _Transaction &tx, u64 k) const {
                if (error_count) return {false, u64()};
                auto am = version_map.find(tx.get_source_txid());

                if (am == version_map.end()) {
                    print_err("invalid version transaction supplied");
                    return {false, u64()};
                }
                auto ai = am->second;
                for (; ai != version_list.end(); ++ai) {
                    auto &a = *ai;
                    auto f = a.boot_map.find(k);
                    if (f != a.boot_map.end()) {
                        return {true, f->second};
                    }
                }
                return get_int_boot_value(k);
            }

            std::string get_name() const {
                return name;
            }
            /**
             * return the allocation record at a logical address
             * the allocation record contains the data size  
             * @tparam _Transaction 
             * @param tx a valid transaction
             * @param l the logical address of the data required
             * @return first==false when no such address is in
             * a) the current transaction
             * b) transactions on which the current is based
             * c) the base file system (which is not technically a transaction)
             * or
             * a) there where pending errors in the error_count member
             */
            template<typename _Transaction>
            std::pair<bool, AllocationRecord> get_alloc(const _Transaction &tx, u64 l) {
                auto am = version_map.find(tx.get_source_txid());

                if (am == version_map.end()) {
                    print_err("invalid version transaction supplied");
                    return {false, {0, 0}};
                }
                auto ai = am->second;
                for (; ai != version_list.end(); ++ai) {
                    auto &a = *ai;
                    auto f = a.allocation_map.find(l);
                    if (f != a.allocation_map.end()) {
                        return {true, f->second};
                    }
                }
                auto r = get_alloc(l);
                return {!r.empty(), r};
            }

            /**
             * starts a transaction and lock resources for it
             * @tparam _Transaction
             * @param tx
             * @return true if operation succeeds
             */
            template<typename _Transaction>
            bool begin(_Transaction &tx) {
                if (error_count) return false;
                if (!tx.set_source_txid(latest_version()->lock_txid())) return false;
                tx.set_version_id(++this->txid_generator);
                return true;
            }
            /**
             * clears any newly changed data in allocation record set representing the transaction
             * @tparam _Transaction 
             * @param tx 
             * @return true if the tx is valid or there are no pending errors
             */
            template<typename _Transaction>
            bool rollback(_Transaction &tx) {
                if (error_count) return false;
                // free all data first while the transaction is actually valid
                tx.iter_alloc([&](u64 /*l*/, AllocationRecord r, AllocationRecord /*ir*/) -> void {
                    // release allocated records here - only newly allocated ones
                    // TODO: free map can be to large in which case it needs to be compacted or something
                    print_dbg(r.to_string());
                    free_map.insert(r); // we hope this always succeeds - should probably check duplicates
                });
                // release lock on mvc list and any allocated data
                if(!unlock_version(tx.get_source_txid())) return false;
                // the transaction is no longe valid and should be cleared  
                tx.clear();
                return true;
            }

            /**
             * commit a transaction - the transaction paramter type must have the following functions:
             * get_txid()
             * iter_write_buf([&](u64 logical, const std::shared_ptr<buffer_type>& buff)
             * iter_alloc([&](u64 l, AllocationRecord r)
             * iter_boot([&](u64 k, u64 v)
             * clear()
             *
             * @tparam _Transaction
             * @param tx
             * @return true on success if false the error_count will not be incremented
             */
            template<typename _Transaction>
            bool commit(_Transaction &tx) {
                if (error_count) return false;

                // TODO: should we really do this so early - I would expect it to happen later
                // i.e. something still missing
                unlock_version(tx.get_source_txid());
                auto latest = latest_version();
                bool new_latest = false;

                auto commit_try = [&]() -> bool {
                    bool abort_commit = false;
                    if (latest->locks > 0) { // if other transactions are looking we have to make an overlay version
                        new_latest = true;
                        latest = create_version(tx.get_version_id());
                    }
                    if (error_count > 0) {
                        return false;
                    }
                    if (abort_commit) return false;
                    // update the allocation table with new addresses for the modified blocks
                    // also copies the version allocation info to the latest internal version structure
                    // ir is the initial record in the case it did not exist the ir.position == o
                    tx.iter_alloc([&](u64 l, AllocationRecord r, AllocationRecord ir) -> void {
                        if (abort_commit) return;

                        auto a = latest->allocation_map.find(l);
                        if (a != latest->allocation_map.end()) {
                           if(a->second.version != ir.version){
                               print_dbg("record has changed by another commit (tx isolation)",a->second.to_string(),ir.to_string());
                               abort_commit = true;
                               return;
                           }
                        }
                        
                        if (!commit_allocation_record(r, l)) {
                            print_err("could not access allocation record at", l);
                            abort_commit = true;
                            return;
                        }
                        
                        if (a != latest->allocation_map.end()) {
                            if (a->second == r) {
                                print_dbg("new address is same as old address aborting commit",a->second.to_string());
                                abort_commit = true;
                                return;
                            }
                            // add old record to free list
                            free_map.insert(a->second);
                        }
                        /// NB this is important and copies the new versions from the incoming transaction
                        /// overwrites if already exists
                        /// a notable side-effect is that the effective version of
                        /// the resource (l) changes - which could cause problems
                        /// if calling structures are using version numbers per resource
                        /// and dont reallocate when source_txid changes
                        latest->allocation_map[l] = r;
                    });
                    if (abort_commit) return false;
                    // iterate through allocations in this tx
                    tx.iter_boot([&](u64 k, u64 v) {
                        if (abort_commit) return;
                        if (!commit_int_boot_value(k, v)) {
                            abort_commit = true;
                            return;
                        }
                        latest->boot_map[k] = v;
                    });
                    return !abort_commit;
                };// lambda commit_try
                bool abort_commit = !commit_try();
                if (abort_commit) {
                    // TODO: try to replay undo log - if undo log cannot be replayed exit process
                    // there is no recovery possible
                    // the new latest should be popped
                    if (new_latest)
                        remove_version(latest->txid); // latest is now invalid
                } else {
                    //TODO: erase the undo log again by writing a starting terminator
                }
                synch();

                tx.clear();//? if there is a failure in the commit should the tx be cleared ?
                return (!abort_commit || error_count > 0);
            }
        };//file_storage_alloc
    }
}

#endif //REPLIFS_FILE_STORAGE_ALLOC_H
