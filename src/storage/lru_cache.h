//
// Created by Pretorius, Christiaan on 2020-07-03.
//

#ifndef REPLIFS_LRU_CACHE_H
#define REPLIFS_LRU_CACHE_H

#include <unordered_map>
#include <list>

template<typename _K, typename _V>
class lru_cache {
public:
    typedef std::list<std::pair<_K, _V>> List; //TODO: a Circular array or circular queue would be better/faster
    typedef std::unordered_map<_K, typename List::iterator> RMap;
private:
    RMap refs;
    List data;
    size_t limit{10};

    template<typename _CallBack>
    void check_limit(_CallBack &&cb) {
        if (refs.size() > limit) { // limit cannot be 0
            auto &b = data.back();
            cb(b.first, b.second);
            erase(b.first);
        }
    }

public:
    lru_cache() {};

    lru_cache(size_t l) : limit(l) {}

    void insert(const _K &k, _V &v) {
        insert(k, v, [&](const _K &, const _V &) {});
    }

    template<typename _CallBack>
    void insert(const _K &k, _V &v, _CallBack &&cb) {
        erase(k);
        
        data.push_front({k, v});
        auto r = data.begin();
        refs[k] = r;
        check_limit(cb);
    }

    bool remove(const _K &k) {
        return ersase(k);
    }

    bool erase(const _K &k) {
        auto f = refs.find(k);
        if (f != refs.end()) {
            data.erase(f->second);
            refs.erase(f);
            return true;
        }
        return false;
    }

    std::pair<bool, _V> find(const _K &k) {
        auto f = refs.find(k);
        if (f != refs.end()) {
            _V v = std::move(f->second->second);
            data.erase(f->second);
            data.push_front({k, v});
            f->second = data.begin();
            return {true, v};
        }
        return {false, _V()};
    }

    size_t count(const _K &k) const {
        return refs.count(k);
    }

    void clear() {
        refs.clear();
        data.clear();
    }

    typedef typename List::iterator iterator;

    typename List::iterator begin() {
        return this->data.begin();
    }

    typename List::const_iterator begin() const {
        return this->data.begin();
    }

    typename List::iterator end() {
        return this->data.end();
    }

    typename List::const_iterator end() const {
        return this->data.end();
    }

    template<typename _CallBack>
    void flush(_CallBack &&cb){
        for(auto i = begin(); i!= end(); ++i){
            cb(i->first,i->second);
        }
        clear();
    }
};

#endif //REPLIFS_LRU_CACHE_H
