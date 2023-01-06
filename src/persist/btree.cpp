
#include "btree.h"

ptrdiff_t btree_totl_used{0};
ptrdiff_t btree_totl_instances{0};
ptrdiff_t btree_totl_surfaces{0};

#include "persist/storage/pool.h"

namespace persist {
    bool memory_low_state{false};
}


namespace persist {
    namespace storage {
        bool storage_debugging{false};
        bool storage_info{false};
    };
};