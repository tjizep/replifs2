//
// Created by Pretorius, Christiaan on 2020-06-13.
//

#ifndef REPLIFS_TEST_H
#define REPLIFS_TEST_H
#include <string>
#include <vector>
#include <iostream>
#include <sstream>
#include <unordered_map>


#include "GraphDB.h"

class test{
    void basic_test(){
        replifs::GraphDB graph{"./replifs_test_data"};
        auto id2 = graph.create();
        std::cout << " id1 " << id1 << " id2 " << id2 << std::endl;
        graph.add(0, id1, "l1", "d1");
        graph.add(0, id2, "l2", "d2");
        graph.add(id1, 0, 0,"LOTS OF BLOCK DATA");
        graph.add(id1, 0, 10,"MORE BLOCK DATA");
        graph.add(id1, 0, 2,"MORE BLOCK DATA");

        graph.add(id1, 0, "s0","LOTS OF BLOCK DATA");
        graph.add(id1, 0, "s1","MORE BLOCK DATA");
        graph.add(id2, 0, 0,"LOTS OF BLOCK DATA");
        graph.add(id2, 0, "s1","MORE BLOCK DATA");

        graph.iterate(0, [&](const GraphDB::_Key& k, GraphDB::_Identity p, const GraphDB::_Value& value) -> bool {
            std::cout << "Name [" << k << "] identity [" << p << "] value [" << value << "]" << std::endl;


            GraphDB::NumberNode number;
            GraphDB::Node text;
            GraphDB::StringData data;
            if(true) {
                auto n = graph.numbers(p, 0);
                for (; n->valid(); n->next()) {
                    n->read(number);
                    n->read(data);
                    std::cout << " Number Leaf [" << number.number << "] [" << data.value << "]" << std::endl;
                }
            }

            if(true){
                auto s = graph.strings(p,"");
                for(;s->valid();s->next()){
                    s->read(text);
                    s->read(data);
                    std::cout << " Text Leaf [" << text.name << "] [" <<  data.value << "]" << std::endl;
                }
            }

            return true;
        });
    }
};
#endif //REPLIFS_TEST_H
