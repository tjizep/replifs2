//
// Created by Pretorius, Christiaan on 2020-06-11.
//

#ifndef REPLIFS_REPLIFS_H
#define REPLIFS_REPLIFS_H

#include <string>
#include <vector>
#include <iostream>
#include <sstream>
#include <unordered_map>


#include "../storage/BTGraphDB.h"

namespace replifs {

    struct Constants {
        enum {
            STAT_OFFSET = 0,
            DATA_OFFSET = 8
        };
    };
    struct File {
        std::string data;
        std::string rbuf;
        struct stat st{0};
    };

    struct Paths {

        static inline bool is_root(const char *path) {
            return (*path != 0x00 && (*path == '/' || *path == '.') && path[1] == 0x00);
        }

        template<typename _F>
        bool parse(const char *path, _F &&cb) {
            const char *ptr = path;
            const char *last_start = path;
            const size_t MAX_PATH = 512;
            char temp[MAX_PATH];
            if (*ptr != '/') return false; // invalid path
            if (!cb("", 0)) {
                return false;
            }
            if (is_root(path)) {
                return true;
            }
            do {
                size_t slashes = 0;
                while (*ptr == '/') { // TODO: can check for whitespace in here as well
                    ++ptr;
                }
                last_start = ptr;
                while (*ptr != '/' && *ptr != 0x00) { // TODO: can check for valid chars too
                    ++ptr;
                }
                size_t len = std::min(MAX_PATH, (size_t) (ptr - last_start));
                memcpy(temp, last_start, len);
                temp[len] = 0x00;
                if (!cb(temp, len)) {
                    return false;
                }
            } while (*ptr != 0x00);
            return true;
        }
    };

    struct DirectoryState {
        typedef replifs::GraphDB::_Identity _Identity;
        size_t alloc{0};
        uint64_t current{0};
        const char *empty_string{""};
        replifs::GraphDB::NumberNode number;
        replifs::GraphDB::Node text;
        replifs::GraphDB::StringData data;
        replifs::GraphDB::prefix_string_iterator si;
        replifs::GraphDB::prefix_number_iterator ni;
        replifs::GraphDB *graph;

        DirectoryState(replifs::GraphDB *graph, size_t al) : alloc(al), graph(graph) {}

        Paths paths;

        uint64_t path2id(const char *path) {
            uint64_t r = 0;
            //if(paths.is_root(path)){ // exception for root
            //    graph->move(si,r,"");
            //    return r;
            //}
            replifs::GraphDB::_Key name;
            bool ok = true;
            paths.parse(path, [&](const char *n, size_t length) {
                name.clear();
                name.append(n, length);
                if ((ok = graph->by_string(data, r, name))) {
                    // TODO: check if data is directory using 'value'
                    r = data.id;
                }
                return ok;
            });
            name.clear();
            if (ok)
                graph->move(si, data.id, name);
            return r;
        }

        bool open(uint64_t id) {
            replifs::GraphDB::_Key name;
            return graph->move(si, id, name);
        }

        bool open(const char *path) {
            auto context = path2id(path);
            return context > 0; //si.valid(); // if the fs is empty this will be false
        }

        const char *current_name() {
            if (si.valid()) {
                si.read(text);
                return text.name.c_str();
            }
            return empty_string;
        }

        _Identity current_id() {
            if (si.valid()) {
                si.read(data);
                return data.id;
            }
            return _Identity();
        }

        bool valid() {
            return si.valid();
        }

        void next() {
            si.next();
        }
    };

    struct resources {
        typedef std::shared_ptr<DirectoryState> DirectoryStatePtr;
        typedef replifs::GraphDB::StringData BlockData;
        typedef replifs::GraphDB::Node Name;
        typedef replifs::GraphDB::NumberNode Number;
        typedef GraphDB::_Identity Identity;

        typedef std::unordered_map<uint64_t, std::shared_ptr<File>> _Statmap;
        typedef std::unordered_map<std::string, uint64_t> _Namemap;
        _Namemap name_data;
        _Statmap stat_data;
        BlockData root_data;
        std::unordered_map<size_t, DirectoryStatePtr> dirs;
        std::vector<DirectoryStatePtr> unused_dirs;
        Paths paths;
        replifs::GraphDB graph{"./replifs_data"};
        size_t alloced{0};
        Identity root;

        resources() {
            if (!graph.opened()) {
                std::cerr << "data store could not be opened" << std::endl;
                return;
            }
            if (graph.is_new()) {
                
                root = graph.create();
                // TODO: probably need some kind og init type here
                struct stat st{0};
                st.st_mode = S_IFDIR | 0755;
                st.st_nlink = 2;
                st.st_ino = root;
                // TODOEND: probably need some kind og init type here
                std::string value;

                graph.add(0, root, "", value);
                set(st);
            } else {
                if (!graph.by_string(root_data, 0, "")) {
                    std::cerr << "could not find root data" << std::endl;
                } else {
                    root = root_data.id;
                }
            }

        }

        DirectoryState *get_dir() {
            DirectoryStatePtr r;
            if (unused_dirs.empty()) {
                r = std::make_shared<DirectoryState>(&graph, ++alloced);
            } else {
                r = unused_dirs.back();
                unused_dirs.pop_back();
            }
            dirs[r->alloc] = r;
            return r.get();
        }

        bool recycle_dir(DirectoryState *dir) {
            if (dir == nullptr) return false;
            if (dirs.count(dir->alloc)) {
                unused_dirs.push_back(dirs[dir->alloc]);
                dirs.erase(dir->alloc);
                return true;
            } else {
                std::cerr << "Directory already freed or invalid" << std::endl;
            }
            return false;
        }

        replifs::File *get_repli_fio(const uint64_t ino) {
            replifs::File *r = nullptr;
            if (ino == 0)
                return r;
            std::shared_ptr<replifs::File> fio = stat_data[ino];
            if (fio == nullptr) {
                fio = std::make_shared<replifs::File>();
                if (this->get(ino, Constants::STAT_OFFSET, (char *) &fio->st, sizeof(fio->st))) {
                    stat_data[ino] = fio;
                } else {
                    return r;
                }

            }
            r = fio.get();
            return r;
        }

        replifs::File *get_repli_fio(const char *path) {
            thread_local std::string p;
            p = path;
            uint64_t inode = name_data[p];
            if (inode == 0) {
                auto r = this->get(path, inode, nullptr, 0);
                if (!r) {
                    return nullptr;
                }
                name_data[p] = inode;
            }
            return get_repli_fio(inode);
        }

        struct _t_inner {
            Identity root;
            Paths paths;
            std::string last;
            std::string name;
            std::string data;
            GraphDB::StringData sdata;

            _t_inner(Identity root) : root(root) {}

            std::pair<bool, uint64_t> path_to_id(GraphDB &graph, const char *path) {
                uint64_t last_id = 0, id = root;
                bool ok = true;
                paths.parse(path, [&](const char *n, size_t length) {
                    name.clear();
                    name.append(n, length);
                    if ((ok = graph.by_string(sdata, id, name))) {
                        // TODO: check if data is directory using 'value'
                        last_id = id;
                        id = sdata.id;
                    }
                    return ok;
                });
                return std::make_pair(ok, id);

            }

            bool set_anon(GraphDB &graph, uint64_t context, uint64_t number, const std::string &d) {
                return graph.add(context, 0, number, d); // will overwrite
            }

            bool set_anon_raw(GraphDB &graph, uint64_t context, uint64_t number, const std::string &d) {
                return graph.add(context, 0, number, d); // will overwrite
            }

            bool set_anon_raw(GraphDB &graph, uint64_t context, uint64_t number, const char *buf, size_t size,
                              size_t intro_offset) {
                return graph.add_raw(context, 0, number, buf, size, intro_offset); // will overwrite
            }

            bool set_anon(GraphDB &graph, uint64_t context, uint64_t number, const char *d, size_t dl) {
                data.clear();
                if (d)
                    data.append(d, dl);
                return graph.add(context, 0, number, data); // will overwrite
            }

            bool set(GraphDB &graph, uint64_t context, uint64_t id, uint64_t number, const std::string &d) {
                return graph.add(context, id, number, d); // will overwrite
            }

            bool set(GraphDB &graph, const char *path, const char *d, size_t dl) {
                uint64_t id = 0, last_id = 0;
                bool ok = true;
                size_t total = 0;
                bool parsed = paths.parse(path, [&](const char *n, size_t length) {
                    name.clear();
                    name.append(n, length);
                    total += length;
                    if ((ok = graph.by_string(sdata, id, name))) {
                        last_id = id;
                        id = sdata.id;
                    }
                    return ok;
                });
                if (parsed) {

                    data.clear();
                    if (d)
                        data.append(d, dl);
                    return graph.add(last_id, id, name, data); // will overwrite
                }
                return false;
            }

            bool remove(GraphDB &graph, uint64_t context, const char *name) {
                return graph.remove(context, name);
            }

            bool remove(GraphDB &graph, uint64_t context, uint64_t number) {
                return graph.remove(context, number);
            }

            bool remove(GraphDB &graph, const char *path) {
                uint64_t id = 0, last_id = 0;
                bool ok = true;
                size_t total = 0;
                bool parsed = paths.parse(path, [&](const char *n, size_t length) {
                    name.clear();
                    if (n)
                        name.append(n, length);
                    total += length;
                    if ((ok = graph.by_string(sdata, id, name))) {
                        last_id = id;
                        id = sdata.id;
                    }
                    return ok;
                });
                if (parsed) {
                    return graph.remove(last_id, name);
                }
                return false;
            }

            bool get(GraphDB &graph, const char *path, std::string &o) {
                uint64_t id = 0, last_id = 0;
                bool ok = true;
                size_t total = 0;
                bool parsed = paths.parse(path, [&](const char *n, size_t length) {
                    name.clear();
                    name.append(n, length);
                    total += length;
                    if ((ok = graph.by_string(sdata, id, name))) {
                        last_id = id;
                        id = sdata.id;
                    }
                    return ok;
                });
                if (parsed) {
                    o = sdata.value;
                }
                return parsed;
            }

            bool get(GraphDB &graph, uint64_t context, const char *n, uint64_t &id) {
                name.clear();
                name.append(n);
                if (graph.by_string(sdata, context, name)) {
                    id = sdata.id;
                    return true;
                }
                return false;
            }

            bool get(GraphDB &graph, uint64_t context, const char *name, char *o, size_t ol) {
                bool ok = true;
                size_t total = 0;
                ok = graph.by_string(sdata, context, name);
                if (ok) {
                    if (o)
                        memcpy(o, sdata.value.data(), std::min<size_t>(sdata.value.size(), ol));
                }
                return ok;
            }

            bool get(GraphDB &graph, uint64_t context, uint64_t number, std::string &o) {
                bool ok = true;
                size_t total = 0;
                ok = graph.by_number(sdata, context, number);
                if (ok) {
                    o = sdata.value.data();
                }
                return ok;
            }

            bool get_raw(GraphDB &graph, uint64_t context, uint64_t number, size_t offset, char *o, size_t ol) {
                return graph.by_number_raw(o, ol, offset, context, number);
            }

            bool get(GraphDB &graph, uint64_t context, uint64_t number, size_t offset, char *o, size_t ol) {
                uint64_t id = 0, last_id = 0;
                bool ok = true;
                size_t total = 0;
                ok = graph.by_number(sdata, context, number);
                if (ok) {
                    if (offset + ol <= sdata.value.size()) {
                        if (o)
                            memcpy(o, sdata.value.data() + offset, std::min<size_t>(sdata.value.size(), ol));
                    } else {
                        return false;
                    }

                }
                return ok;
            }

            bool get(GraphDB &graph, const char *path, uint64_t &oid, char *o, size_t ol) {
                uint64_t id = 0, last_id = 0;
                bool ok = true;
                size_t total = 0;
                bool parsed = paths.parse(path, [&](const char *n, size_t length) {
                    name.clear();
                    name.append(n, length);
                    total += length;
                    size_t sds = sdata.value.size();
                    if ((ok = graph.by_string(sdata, id, name))) {
                        size_t sds2 = sdata.value.size();
                        last_id = id;
                        id = sdata.id;
                    }
                    return ok;
                });
                if (parsed) {
                    if (o)
                        memcpy(o, sdata.value.data(), std::min<size_t>(sdata.value.size(), ol));
                    oid = id;
                }
                return parsed;
            }

            std::pair<bool, uint64_t> create(GraphDB &graph, const char *path, const char *d, size_t dl) {
                uint64_t cid = graph.create();
                return create(graph, path, cid, d, dl);
            }

            std::pair<bool, uint64_t>
            create(GraphDB &graph, uint64_t context, const std::string &name, uint64_t cid, const char *d, size_t dl) {
                data.clear();
                if (d)
                    data.append(d, dl);
                return std::make_pair(graph.add(context, cid, name, data), cid);
            }

            std::pair<bool, uint64_t>
            create(GraphDB &graph, uint64_t context, const char *n, uint64_t cid, const char *d, size_t dl) {
                name = n;
                return create(graph, context, name, cid, d, dl);
            }

            std::pair<bool, uint64_t> create(GraphDB &graph, const char *path, uint64_t cid, const char *d, size_t dl) {
                uint64_t id = 0, last_id = 0;
                uint64_t r = 0;
                uint32_t lookups = 0, lookup_failures = 0, last_fail = 0xFFFFFFFFul;
                bool ok = true, last_resolved = true;
                size_t total = 0;
                bool parsed = paths.parse(path, [&](const char *n, size_t length) {
                    ++lookups;
                    name.clear();
                    name.append(n, length);
                    total += length;
                    if (graph.by_string(sdata, id, name)) {
                        id = sdata.id;
                    } else {
                        ++lookup_failures;
                        last_fail = lookups;
                    }
                    return true;
                });
                if (parsed && lookup_failures <= 1 && last_fail >= lookups) {
                    return create(graph, id, name, cid, d, dl);
                } else {
                    parsed = false;
                }

                return std::make_pair(parsed, cid);
            }
        };

        _t_inner &get_local() {
            thread_local _t_inner tl{root};
            assert(tl.root == root);
            return tl;
        }

        std::pair<bool, uint64_t> path_to_id(const char *path) {
            _t_inner &local = get_local();
            return local.path_to_id(graph, path);
        }

        bool set(uint64_t context, uint64_t id, uint64_t number, const std::string &d) {
            _t_inner &local = get_local();
            return local.set(graph, context, id, number, d);
        }

        bool set_anon(uint64_t context, uint64_t number, const std::string &d) {
            _t_inner &local = get_local();
            return local.set_anon(graph, context, number, d);
        }

        bool set_anon_raw(uint64_t context, uint64_t number, const std::string &d) {
            _t_inner &local = get_local();
            return local.set_anon_raw(graph, context, number, d);
        }

        bool set_anon_raw(uint64_t context, uint64_t number, const char *buf, size_t size, size_t intro_offset) {
            _t_inner &local = get_local();
            return local.set_anon_raw(graph, context, number, buf, size, intro_offset);
        }

        bool remove(uint64_t context, uint64_t number) {
            _t_inner &local = get_local();
            return local.remove(graph, context, number);
        }

        bool remove(uint64_t context, const char *name) {
            _t_inner &local = get_local();
            return local.remove(graph, context, name);
        }

        bool remove(const char *path) {
            _t_inner &local = get_local();
            bool r = local.remove(graph, path);
            if (r) {
                stat_data.erase(name_data[path]);
            }
            return r;
        }

        bool set(const struct stat &data) {
            if (data.st_ino == 0) {
                return false;
            }
            _t_inner &local = get_local();
            return local.set_anon(graph, (uint64_t) data.st_ino, Constants::STAT_OFFSET, (const char *) &data,
                                  sizeof(struct stat));
        }

        bool set(const File *fio) {
            if (!fio) return false;
            return set(fio->st);
        }

        bool set(const char *path, const char *d, size_t dl) {
            _t_inner &local = get_local();
            return local.set(graph, path, d, dl);
        }

        bool get(const char *path, std::string &o) {
            _t_inner &local = get_local();
            return local.get(graph, path, o);
        }

        bool get(const char *path, char *o, size_t ol) {
            _t_inner &local = get_local();
            uint64_t id = 0;
            return local.get(graph, path, id, o, ol);
        }

        bool get(const char *path, uint64_t &id, char *o, size_t ol) {
            _t_inner &local = get_local();
            return local.get(graph, path, id, o, ol);
        }

        bool get(uint64_t context, const char *name, char *o, size_t ol) {
            _t_inner &local = get_local();
            return local.get(graph, context, name, o, ol);
        }

        bool get(uint64_t context, const char *name, uint64_t &id) {
            _t_inner &local = get_local();
            return local.get(graph, context, name, id);
        }

        bool get(uint64_t context, uint64_t number, char *o, size_t ol) {
            _t_inner &local = get_local();
            return local.get(graph, context, number, 0, o, ol);
        }

        bool get(uint64_t context, uint64_t number, size_t offset, char *o, size_t ol) {
            _t_inner &local = get_local();
            return local.get(graph, context, number, offset, o, ol);
        }

        bool get_raw(uint64_t context, uint64_t number, size_t offset, char *o, size_t ol) {
            _t_inner &local = get_local();
            return local.get_raw(graph, context, number, offset, o, ol);
        }

        bool get(uint64_t context, uint64_t number, std::string &o) {
            _t_inner &local = get_local();
            return local.get(graph, context, number, o);
        }

        std::pair<bool, uint64_t> create(const char *path, const char *d, size_t dl) {
            _t_inner &local = get_local();
            return local.create(graph, path, d, dl);
        }

        std::pair<bool, uint64_t> create(const char *path, uint64_t cid, const char *d, size_t dl) {
            _t_inner &local = get_local();
            return local.create(graph, path, cid, d, dl);
        }

        std::pair<bool, uint64_t> create(uint64_t context, const char *name, uint64_t cid, const char *d, size_t dl) {
            _t_inner &local = get_local();
            return local.create(graph, context, name, cid, d, dl);
        }

        uint64_t create() {
            return graph.create();
        }
    };

}
#endif //REPLIFS_REPLIFS_H
